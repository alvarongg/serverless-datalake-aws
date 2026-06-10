"""Unit tests (ejemplos y casos de error) del Glue Job ``transform_job``.

Cubre los casos de error de los Requisitos 3.4 y 3.5 con mocks del cliente S3 de boto3
(``unittest.mock``), sin red ni recursos AWS reales:

- **Requisito 3.4**: el job falla *sin escribir nada* en ``processed/`` cuando la key no
  apunta a un ``.csv``, cuando el objeto no existe, o cuando el contenido no es un CSV
  parseable.
- **Requisito 3.5**: si la escritura del Parquet falla a mitad de la promoción a
  ``processed/``, no quedan particiones parciales: se limpia el staging y la zona final
  ``processed/ventas/`` queda vacía.

Estos tests complementan los tests de propiedad de ``extraer_fecha`` y de la
transformación CSV→Parquet (Properties 2 y 3): aquí se verifican ejemplos concretos de
fallo en lugar de propiedades universales.
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from transform_job import (
    PREFIJO_DESTINO,
    PREFIJO_STAGING,
    escribir_parquet_atomico,
    leer_csv,
    leer_csv_desde_bytes,
)


class FakeS3Client:
    """Cliente S3 falso en memoria para verificar el estado del bucket tras un fallo.

    Por qué: para comprobar el Requisito 3.5 no basta con un ``MagicMock`` que registre
    llamadas; necesitamos un modelo del estado del bucket que refleje qué objetos quedan
    realmente escritos. Este fake mantiene un diccionario ``key -> contenido`` y permite
    inyectar un fallo en la promoción (``copy_object``) para simular una escritura
    interrumpida a mitad de camino.
    """

    def __init__(self, fallar_copy_en: int | None = None):
        # Estado del "bucket": claves actualmente presentes.
        self.objetos: dict[str, bytes] = {}
        # Si se indica, ``copy_object`` lanza al alcanzar esa cantidad de invocaciones
        # (1 = falla en la primera promoción), simulando un fallo a mitad de escritura.
        self._fallar_copy_en = fallar_copy_en
        self._copias = 0

    def upload_file(self, ruta_local: str, bucket: str, key: str) -> None:
        """Sube un archivo local al staging (lee el contenido real del temporal)."""
        with open(ruta_local, "rb") as archivo:
            self.objetos[key] = archivo.read()

    def copy_object(self, Bucket: str, CopySource: dict, Key: str) -> None:
        """Promueve un objeto de staging a su destino final; puede fallar a propósito."""
        self._copias += 1
        if self._fallar_copy_en is not None and self._copias >= self._fallar_copy_en:
            raise RuntimeError("Fallo simulado de S3 durante la promoción a processed/.")
        self.objetos[Key] = self.objetos[CopySource["Key"]]

    def delete_object(self, Bucket: str, Key: str) -> None:
        """Borra un objeto (usado para limpiar el staging)."""
        self.objetos.pop(Key, None)

    # --- Helpers de verificación para los tests ---
    def claves_con_prefijo(self, prefijo: str) -> list[str]:
        """Devuelve las claves presentes que empiezan con ``prefijo``."""
        return [clave for clave in self.objetos if clave.startswith(prefijo)]


def _df_ventas_minimo() -> pd.DataFrame:
    """Construye un DataFrame de ventas ya transformado (con columna ``fecha``).

    Por qué: ``escribir_parquet_atomico`` particiona por la columna ``fecha``; el
    DataFrame de prueba debe incluirla para reproducir el flujo real de escritura.
    """
    return pd.DataFrame(
        {
            "sucursal": ["centro", "norte"],
            "producto": ["café", "té"],
            "cantidad": [3, 5],
            "monto": [12.50, 8.00],
            "fecha": ["2025-06-10", "2025-06-10"],
        }
    )


# --------------------------------------------------------------------------------------
# Requisito 3.4: lectura/validación que falla sin escribir en processed/
# --------------------------------------------------------------------------------------


def test_leer_csv_key_sin_extension_csv_lanza_sin_tocar_s3():
    """Una key que no termina en ``.csv`` lanza ``ValueError`` antes de llamar a S3.

    Verifica el Requisito 3.4: la validación temprana evita cualquier acceso a S3 (no se
    invoca ``get_object``), de modo que el job falla sin leer ni escribir nada.
    """
    s3 = MagicMock()

    with pytest.raises(ValueError, match=r"\.csv"):
        leer_csv("mi-bucket", "raw/ventas/ventas-2025-06-10.txt", s3_client=s3)

    # No debe haberse intentado ninguna operación contra S3.
    s3.get_object.assert_not_called()


def test_leer_csv_key_inexistente_propaga_error_y_no_escribe():
    """Si el objeto no existe, ``get_object`` falla y el error se propaga sin escribir.

    Verifica el Requisito 3.4: ante una key inexistente el job termina con fallo y no
    escribe nada (no se invocan ``upload_file`` ni ``copy_object``).
    """
    s3 = MagicMock()
    # Simula la excepción que boto3 lanzaría ante una key inexistente.
    s3.get_object.side_effect = Exception("NoSuchKey: la key no existe en el bucket.")

    with pytest.raises(Exception, match="NoSuchKey"):
        leer_csv("mi-bucket", "raw/ventas/ventas-2025-06-10.csv", s3_client=s3)

    # La lectura no escribe nada en processed/.
    s3.upload_file.assert_not_called()
    s3.copy_object.assert_not_called()


def test_leer_csv_desde_bytes_contenido_invalido_lanza_valueerror():
    """Contenido que no es un CSV parseable lanza ``ValueError`` (Requisito 3.4).

    Se usan bytes binarios no decodificables como UTF-8 para forzar el fallo de parseo.
    """
    contenido_invalido = b"\xff\xfe\x00\x01\x02\x03binario-no-csv"

    with pytest.raises(ValueError):
        leer_csv_desde_bytes(contenido_invalido)


def test_leer_csv_desde_bytes_vacio_lanza_valueerror():
    """Contenido vacío no tiene datos que transformar: lanza ``ValueError`` (Req. 3.4)."""
    with pytest.raises(ValueError):
        leer_csv_desde_bytes(b"")


# --------------------------------------------------------------------------------------
# Requisito 3.5: fallo de escritura a mitad de camino sin dejar particiones parciales
# --------------------------------------------------------------------------------------


def test_escritura_atomica_falla_a_mitad_no_deja_particiones_parciales():
    """Si la promoción a ``processed/`` falla, no quedan datos en ``processed/ventas/``.

    Verifica el Requisito 3.5: ante un fallo de ``copy_object`` (escritura interrumpida),
    ``escribir_parquet_atomico`` relanza el error, limpia el staging y deja la zona final
    ``processed/ventas/`` sin objetos parciales.
    """
    df = _df_ventas_minimo()
    # El fake falla en la primera promoción (copy_object), simulando el fallo a mitad.
    s3 = FakeS3Client(fallar_copy_en=1)

    with pytest.raises(RuntimeError, match="Fallo simulado"):
        escribir_parquet_atomico(
            df,
            bucket="mi-bucket",
            fecha="2025-06-10",
            run_id="run-test-123",
            s3_client=s3,
        )

    # No debe quedar ningún objeto bajo la zona final processed/ventas/.
    assert s3.claves_con_prefijo(PREFIJO_DESTINO) == []
    # El staging tampoco debe quedar con objetos (se limpió en el finally).
    assert s3.claves_con_prefijo(PREFIJO_STAGING) == []


def test_escritura_atomica_falla_limpia_el_staging_creado():
    """El staging usado durante el intento fallido queda limpio (sin basura residual).

    Verifica el Requisito 3.5 desde la perspectiva del staging: aunque la subida al
    staging haya ocurrido, tras el fallo no debe quedar ningún objeto bajo
    ``processed/_staging/<run-id>/``.
    """
    df = _df_ventas_minimo()
    s3 = FakeS3Client(fallar_copy_en=1)

    with pytest.raises(RuntimeError):
        escribir_parquet_atomico(
            df,
            bucket="mi-bucket",
            fecha="2025-06-10",
            run_id="run-test-456",
            s3_client=s3,
        )

    prefijo_staging_run = f"{PREFIJO_STAGING}/run-test-456"
    assert s3.claves_con_prefijo(prefijo_staging_run) == []
