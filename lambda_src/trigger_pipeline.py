"""Trigger_Lambda: orquesta el pipeline iniciando el Glue Job ante eventos de S3.

Por qué esta Lambda solo orquesta y no transforma datos: en la arquitectura
event-driven cada servicio hace una sola cosa. El almacenamiento (S3) notifica, esta
función orquesta (inicia el Glue Job) y el Glue Job transforma. La Lambda nunca lee,
escribe, mueve ni borra el objeto en ``raw/``: solo dispara la transformación.

Diseño orientado a una función pura y testeable:

- ``debe_procesar`` : decisión de filtrado (prefijo ``raw/`` + sufijo ``.csv``), sin
  dependencias de AWS, para poder validarla con property-based testing (Property 1).
- ``handler``       : punto de entrada de Lambda; extrae bucket/key del evento S3 e
  inicia exactamente una ejecución del Glue Job por cada objeto que debe procesarse.
"""

import logging
import os
from urllib.parse import unquote

import boto3

# Cliente de Glue reutilizado entre invocaciones (se crea una vez por contenedor).
glue = boto3.client("glue")

# Logger en español para facilitar el diagnóstico (objetivo educativo del proyecto).
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def debe_procesar(key):
    """Decide si una key de S3 debe disparar el Glue Job.

    Por qué es una función pura aislada: la notificación de S3 ya filtra por prefijo
    ``raw/`` y sufijo ``.csv``, pero replicar la decisión aquí como función sin
    dependencias de AWS permite (a) defender en profundidad ante eventos que lleguen
    pese al filtro y (b) testear la regla con property-based testing (Property 1).

    Devuelve ``True`` si y solo si la key empieza con ``raw/`` y termina con ``.csv``;
    cualquier otra key (incluidas las de ``processed/``, ``curated/`` o las de ``raw/``
    sin sufijo ``.csv``) devuelve ``False``.
    """
    return key.startswith("raw/") and key.endswith(".csv")


def handler(event, context):
    """Inicia el Glue Job por cada objeto ``raw/*.csv`` presente en el evento S3.

    Por qué relanza la excepción ante un fallo: devolver un estado de error al
    invocador permite que el mecanismo de reintentos actúe y deja registro del
    problema. La Lambda no toca el objeto en ``raw/`` (no lo borra ni lo mueve), de
    modo que el dato crudo se preserva intacto para un reproceso posterior.
    """
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        # La key viaja URL-encoded en el evento de S3 (p. ej. espacios como '+'/%20);
        # se decodifica para obtener la key real del objeto.
        key = unquote(record["s3"]["object"]["key"])

        # Defensa en profundidad adicional al filtro de la notificación S3
        # (Requisitos 2.3 y 2.6): si la key no corresponde a un CSV de raw/, se ignora.
        if not debe_procesar(key):
            continue

        try:
            # Se inicia exactamente una ejecución del Glue Job, pasando el bucket y la
            # key como argumentos para que el job sepa qué objeto transformar.
            glue.start_job_run(
                JobName=os.environ["GLUE_JOB_NAME"],
                Arguments={"--bucket": bucket, "--key": key},
            )
        except Exception as e:
            # Se registra el contexto relevante (bucket y key) y se relanza la
            # excepción para devolver estado de error al invocador. El objeto en raw/
            # permanece intacto (Requisito 2.5).
            logger.error(
                "No se pudo iniciar el Glue Job para %s/%s: %s", bucket, key, e
            )
            raise
