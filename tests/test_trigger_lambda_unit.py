"""Unit tests (ejemplos y casos de error) de la Trigger_Lambda ``trigger_pipeline``.

Cubre los Requisitos 2.4 y 2.5 con mocks del cliente Glue de boto3
(``unittest.mock``), sin red ni recursos AWS reales:

- **Requisito 2.4**: ante un evento S3 con un objeto ``raw/*.csv``, el handler inicia
  *exactamente una* ejecución del Glue Job, con el ``JobName`` correcto (el de la
  variable de entorno ``GLUE_JOB_NAME``) y los ``Arguments`` ``--bucket`` y ``--key``
  esperados. La key se entrega decodificada (URL-decode) tal como llega en el evento.
- **Requisito 2.5**: si ``start_job_run`` falla, el handler relanza la excepción y *no*
  toca el objeto en ``raw/`` (la Lambda no tiene cliente S3, así que solo orquesta).

Sobre la importación del módulo: ``trigger_pipeline`` crea un ``boto3.client("glue")``
en tiempo de importación. El ``conftest.py`` de la suite agrega ``lambda_src/`` al
``sys.path`` y fija una región AWS por defecto antes de importar, de modo que el
``import`` funcione en un entorno sin credenciales/perfil AWS (p. ej. en CI).

Estos tests complementan los tests de propiedad de ``debe_procesar`` (Property 1): aquí
se verifican ejemplos concretos del orquestado del Glue Job en lugar de la regla de
filtrado universal.
"""

from unittest.mock import patch

import pytest

import trigger_pipeline

# Nombre de Glue Job de prueba que el handler debe usar como ``JobName`` (Req. 2.4).
NOMBRE_JOB = "datalake-transform-job-test"


def _evento_s3(bucket: str, key: str) -> dict:
    """Construye un evento S3 OBJECT_CREATED mínimo con un único record.

    Por qué: el handler solo lee ``record["s3"]["bucket"]["name"]`` y
    ``record["s3"]["object"]["key"]`` de cada record; el evento de prueba incluye
    exactamente esa estructura para reproducir la forma real del evento de S3.
    """
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key},
                }
            }
        ]
    }


# --------------------------------------------------------------------------------------
# Requisito 2.4: inicia exactamente una ejecución con JobName y Arguments correctos
# --------------------------------------------------------------------------------------


def test_handler_inicia_una_ejecucion_con_jobname_y_arguments_correctos():
    """Un evento con un objeto ``raw/*.csv`` dispara exactamente un ``start_job_run``.

    Verifica el Requisito 2.4: el handler invoca el Glue Job una sola vez, con el
    ``JobName`` tomado de ``GLUE_JOB_NAME`` y los ``Arguments`` ``--bucket``/``--key``
    apuntando al objeto del evento.
    """
    bucket = "mi-datalake-bucket"
    key = "raw/ventas/ventas-2025-06-10.csv"
    evento = _evento_s3(bucket, key)

    with patch.dict("os.environ", {"GLUE_JOB_NAME": NOMBRE_JOB}):
        with patch.object(trigger_pipeline, "glue") as glue_mock:
            trigger_pipeline.handler(evento, context=None)

    # Exactamente una ejecución del Glue Job (Requisito 2.4).
    glue_mock.start_job_run.assert_called_once_with(
        JobName=NOMBRE_JOB,
        Arguments={"--bucket": bucket, "--key": key},
    )


def test_handler_decodifica_la_key_url_encoded_antes_de_pasarla_al_job():
    """La key llega URL-encoded en el evento S3 y se entrega decodificada al Glue Job.

    Verifica el Requisito 2.4: S3 codifica caracteres especiales (p. ej. espacios como
    ``%20``) en la key del evento; el handler la decodifica con ``unquote`` para que el
    job reciba la key real del objeto.
    """
    bucket = "mi-datalake-bucket"
    # Key codificada tal como viajaría en el evento de S3: '%20' representa un espacio.
    key_codificada = "raw/ventas/ventas-2025-06-10%20copia.csv"
    key_esperada = "raw/ventas/ventas-2025-06-10 copia.csv"
    evento = _evento_s3(bucket, key_codificada)

    with patch.dict("os.environ", {"GLUE_JOB_NAME": NOMBRE_JOB}):
        with patch.object(trigger_pipeline, "glue") as glue_mock:
            trigger_pipeline.handler(evento, context=None)

    glue_mock.start_job_run.assert_called_once_with(
        JobName=NOMBRE_JOB,
        Arguments={"--bucket": bucket, "--key": key_esperada},
    )


def test_handler_ignora_records_que_no_son_raw_csv():
    """Records que no son ``raw/*.csv`` no disparan el Glue Job (defensa en profundidad).

    Aunque la notificación de S3 ya filtra por prefijo/sufijo, el handler vuelve a
    aplicar la decisión de filtrado: un objeto en ``processed/`` o un ``.txt`` en
    ``raw/`` no debe iniciar ninguna ejecución del job.
    """
    evento = {
        "Records": [
            {"s3": {"bucket": {"name": "b"}, "object": {"key": "raw/foo.txt"}}},
            {"s3": {"bucket": {"name": "b"}, "object": {"key": "processed/foo.csv"}}},
        ]
    }

    with patch.dict("os.environ", {"GLUE_JOB_NAME": NOMBRE_JOB}):
        with patch.object(trigger_pipeline, "glue") as glue_mock:
            trigger_pipeline.handler(evento, context=None)

    # Ningún record coincide con raw/*.csv: el job nunca se inicia.
    glue_mock.start_job_run.assert_not_called()


# --------------------------------------------------------------------------------------
# Requisito 2.5: ante fallo de start_job_run, relanza el error y no toca el objeto
# --------------------------------------------------------------------------------------


def test_handler_relanza_error_si_start_job_run_falla():
    """Si ``start_job_run`` lanza, el handler propaga la excepción al invocador.

    Verifica el Requisito 2.5: el fallo al iniciar el Glue Job se relanza (devuelve
    estado de error al invocador), permitiendo el reintento y dejando registro.
    """
    evento = _evento_s3("mi-datalake-bucket", "raw/ventas/ventas-2025-06-10.csv")

    with patch.dict("os.environ", {"GLUE_JOB_NAME": NOMBRE_JOB}):
        with patch.object(trigger_pipeline, "glue") as glue_mock:
            glue_mock.start_job_run.side_effect = RuntimeError(
                "ConcurrentRunsExceededException: límite de ejecuciones del job."
            )

            with pytest.raises(RuntimeError, match="ConcurrentRunsExceeded"):
                trigger_pipeline.handler(evento, context=None)

            # Se intentó iniciar el job exactamente una vez antes de fallar.
            glue_mock.start_job_run.assert_called_once()


def test_handler_no_borra_ni_muta_el_objeto_ante_fallo():
    """Ante un fallo del Glue Job, la Lambda no borra ni modifica el objeto en ``raw/``.

    Verifica el Requisito 2.5: el dato crudo se preserva intacto para un reproceso. La
    Lambda no posee cliente S3 (solo orquesta), de modo que verificamos que el único
    contacto con AWS fue el intento de ``start_job_run`` sobre el cliente Glue: no se
    invocó ninguna otra operación (p. ej. ``delete_object``) sobre el mock.
    """
    evento = _evento_s3("mi-datalake-bucket", "raw/ventas/ventas-2025-06-10.csv")

    with patch.dict("os.environ", {"GLUE_JOB_NAME": NOMBRE_JOB}):
        with patch.object(trigger_pipeline, "glue") as glue_mock:
            glue_mock.start_job_run.side_effect = RuntimeError("Fallo simulado de Glue.")

            with pytest.raises(RuntimeError):
                trigger_pipeline.handler(evento, context=None)

    # El único método invocado sobre cualquier cliente AWS fue start_job_run.
    metodos_invocados = [nombre for nombre, _, _ in glue_mock.mock_calls]
    assert metodos_invocados == ["start_job_run"]
    # En particular, no hubo ninguna operación de borrado/mutación del objeto.
    glue_mock.delete_object.assert_not_called()
