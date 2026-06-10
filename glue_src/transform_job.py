"""Glue Job de transformación: CSV crudo (raw/) -> Parquet particionado (processed/).

Por qué este job es un **Glue Python Shell** y no Spark: el escenario maneja un CSV
de ventas diario y pequeño (100–1000 filas). Python Shell con pandas + pyarrow es más
simple, más barato y más legible (objetivo educativo del proyecto). Tradeoff: no escala
a archivos muy grandes; si los volúmenes crecen, se migra a Spark/Glue ETL.

Diseño orientado a funciones puras y testeables:

- ``extraer_fecha``        : deriva la fecha AAAA-MM-DD del nombre del archivo de origen.
- ``leer_csv_desde_bytes`` : parsea el contenido CSV (lógica pura, sin AWS).
- ``leer_csv``             : descarga el objeto de S3 y delega en ``leer_csv_desde_bytes``.
- ``transformar``          : fija la columna de partición ``fecha`` con la fecha de origen.
- ``escribir_dataset_parquet_local`` : escribe el dataset Parquet particionado en disco.
- ``escribir_parquet_atomico``       : escritura atómica a processed/ vía staging en S3.

La separación permite que las tasks 3.3/3.4/3.5 escriban tests de propiedad y unitarios
contra la lógica pura sin necesidad de un entorno Glue real.
"""

import io
import logging
import os
import re
import shutil
import sys
import tempfile
import uuid

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# El módulo ``awsglue`` solo existe en el runtime de Glue. Se importa de forma
# defensiva para que el módulo pueda importarse en tests/local sin ese paquete.
try:  # pragma: no cover - depende del entorno de ejecución
    from awsglue.utils import getResolvedOptions
except ImportError:  # pragma: no cover
    getResolvedOptions = None

# Logger en español para diagnóstico (objetivo educativo del proyecto).
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("transform_job")

# Patrón de fecha AAAA-MM-DD embebida en el nombre del archivo de origen.
_PATRON_FECHA = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

# Nombre de la columna de partición. Coincide con la convención Hive del path de salida
# (``processed/ventas/fecha=<AAAA-MM-DD>/``) que luego infiere el Glue Crawler.
COLUMNA_PARTICION = "fecha"

# Prefijos de la convención de zonas del bucket.
PREFIJO_DESTINO = "processed/ventas"
PREFIJO_STAGING = "processed/_staging"


def extraer_fecha(nombre_archivo: str) -> str:
    """Extrae la fecha AAAA-MM-DD embebida en el nombre del archivo de origen.

    Por qué: la partición se deriva del nombre del archivo (p. ej.
    ``ventas-2025-06-10.csv``), no del contenido, para que la ubicación en el lake
    sea predecible. Valida que la fecha exista y sea un calendario real; si no, lanza
    error para que el job falle antes de escribir nada (Requisito 3.4).

    Args:
        nombre_archivo: nombre o key del archivo de origen (puede incluir prefijos).

    Returns:
        La fecha en formato ``AAAA-MM-DD``.

    Raises:
        ValueError: si no hay una fecha AAAA-MM-DD válida en el nombre.
    """
    if not nombre_archivo:
        raise ValueError("El nombre de archivo está vacío; no se puede extraer la fecha.")

    coincidencia = _PATRON_FECHA.search(nombre_archivo)
    if coincidencia is None:
        raise ValueError(
            f"El nombre '{nombre_archivo}' no contiene una fecha AAAA-MM-DD válida."
        )

    anio, mes, dia = (int(parte) for parte in coincidencia.groups())
    # Validar que sea una fecha de calendario real (rechaza 2025-13-40, etc.).
    import datetime

    try:
        datetime.date(anio, mes, dia)
    except ValueError as error:
        raise ValueError(
            f"El nombre '{nombre_archivo}' contiene una fecha inválida: {error}."
        ) from error

    return coincidencia.group(0)


def leer_csv_desde_bytes(contenido: bytes) -> pd.DataFrame:
    """Parsea contenido CSV en bytes a un ``DataFrame`` (lógica pura, sin AWS).

    Por qué: aislar el parseo del acceso a S3 permite testear el manejo de CSV válidos
    e inválidos sin red. Si el contenido no es un CSV parseable o está vacío, lanza
    error para que el job falle sin escribir nada (Requisito 3.4).

    Args:
        contenido: bytes del archivo CSV.

    Returns:
        El ``DataFrame`` con todas las filas y columnas del CSV.

    Raises:
        ValueError: si el contenido está vacío o no puede leerse como CSV válido.
    """
    if not contenido:
        raise ValueError("El contenido del CSV está vacío; no hay datos que transformar.")

    try:
        df = pd.read_csv(io.BytesIO(contenido))
    except (pd.errors.EmptyDataError, pd.errors.ParserError, UnicodeDecodeError) as error:
        raise ValueError(f"El contenido no es un CSV válido: {error}.") from error

    if df.empty:
        raise ValueError("El CSV no contiene filas de datos.")

    return df


def leer_csv(bucket: str, key: str, s3_client=None) -> pd.DataFrame:
    """Descarga un CSV de S3 y lo parsea a un ``DataFrame``.

    Por qué: separa la E/S de S3 (testeable con mocks) del parseo puro. Falla si la key
    no termina en ``.csv``, si el objeto no existe o si el contenido no es CSV válido,
    sin escribir nada en ``processed/`` (Requisito 3.4).

    Args:
        bucket: nombre del bucket de origen.
        key: key del objeto CSV dentro de la Zona Raw.
        s3_client: cliente boto3 de S3 inyectable (se crea uno si es ``None``).

    Returns:
        El ``DataFrame`` con el contenido del CSV.

    Raises:
        ValueError: si la key no apunta a un ``.csv`` o el contenido no es válido.
    """
    if not key.endswith(".csv"):
        raise ValueError(f"La key '{key}' no apunta a un archivo .csv.")

    cliente = s3_client or boto3.client("s3")
    respuesta = cliente.get_object(Bucket=bucket, Key=key)
    contenido = respuesta["Body"].read()
    return leer_csv_desde_bytes(contenido)


def transformar(df: pd.DataFrame, fecha: str) -> pd.DataFrame:
    """Prepara el ``DataFrame`` para escritura particionada por la fecha de origen.

    Por qué: la partición ``fecha`` proviene del nombre del archivo, no del contenido.
    Se fija la columna ``fecha`` de todas las filas a esa fecha para garantizar una única
    partición coherente con el archivo (Requisito 3.3). Se preservan todas las demás
    columnas y filas del CSV de origen. Opera sobre una copia para no mutar la entrada.

    Args:
        df: ``DataFrame`` con los datos del CSV de origen.
        fecha: fecha de partición en formato ``AAAA-MM-DD`` (de ``extraer_fecha``).

    Returns:
        Un nuevo ``DataFrame`` con la columna de partición ``fecha`` fijada.
    """
    resultado = df.copy()
    resultado[COLUMNA_PARTICION] = fecha
    return resultado


def escribir_dataset_parquet_local(df: pd.DataFrame, directorio: str) -> None:
    """Escribe el ``DataFrame`` como dataset Parquet particionado en disco local.

    Por qué: pyarrow codifica la columna de partición en el path
    (``fecha=<AAAA-MM-DD>/``) siguiendo la convención Hive y la omite del cuerpo del
    Parquet; al volver a leer con descubrimiento de particiones se reconstruye idéntica.
    Esto preserva todas las filas y columnas del origen y mantiene la fecha como
    partición (Requisito 3.3). Se escribe primero en local para luego promover de forma
    atómica a S3.

    Args:
        df: ``DataFrame`` ya transformado (con columna ``fecha``).
        directorio: directorio base donde se escribe el dataset particionado.
    """
    tabla = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        tabla,
        root_path=directorio,
        partition_cols=[COLUMNA_PARTICION],
    )


def escribir_parquet_atomico(
    df: pd.DataFrame,
    bucket: str,
    fecha: str,
    run_id: str,
    s3_client=None,
) -> str:
    """Escribe el Parquet en ``processed/`` de forma atómica vía staging.

    Por qué (Requisito 3.5): si la escritura falla a mitad de camino no deben quedar
    particiones parciales en ``processed/``. Estrategia:

    1. Se escribe el dataset particionado en un directorio temporal local.
    2. Se sube cada archivo a ``processed/_staging/<run-id>/`` (zona de staging).
    3. Solo cuando todo el staging está completo, se copian los objetos a su ubicación
       final ``processed/ventas/fecha=<AAAA-MM-DD>/`` (copia S3 por objeto, atómica).
    4. Se limpia el staging.

    Ante cualquier fallo se limpia el staging y se relanza el error, de modo que
    ``processed/ventas/`` nunca queda con datos parciales.

    Args:
        df: ``DataFrame`` ya transformado (con columna ``fecha``).
        bucket: bucket de destino.
        fecha: fecha de partición ``AAAA-MM-DD``.
        run_id: identificador único de la ejecución (para aislar el staging).
        s3_client: cliente boto3 de S3 inyectable (se crea uno si es ``None``).

    Returns:
        El prefijo final de destino en ``processed/`` donde quedaron los datos.

    Raises:
        Exception: cualquier error de escritura; el staging se limpia antes de relanzar.
    """
    cliente = s3_client or boto3.client("s3")
    directorio_local = tempfile.mkdtemp(prefix="glue_transform_")
    prefijo_staging = f"{PREFIJO_STAGING}/{run_id}"
    claves_staging: list[str] = []

    try:
        # 1. Escritura local del dataset particionado.
        escribir_dataset_parquet_local(df, directorio_local)

        # 2. Subir cada archivo generado al staging, preservando la ruta relativa
        #    (que incluye la carpeta de partición ``fecha=<AAAA-MM-DD>/``).
        archivos = _listar_archivos(directorio_local)
        for ruta_absoluta in archivos:
            ruta_relativa = os.path.relpath(ruta_absoluta, directorio_local)
            key_staging = f"{prefijo_staging}/{ruta_relativa}"
            cliente.upload_file(ruta_absoluta, bucket, key_staging)
            claves_staging.append(key_staging)

        # 3. Promover de staging a la ubicación final (copia atómica por objeto).
        prefijo_destino = f"{PREFIJO_DESTINO}"
        for key_staging in claves_staging:
            ruta_relativa = os.path.relpath(key_staging, prefijo_staging)
            key_destino = f"{prefijo_destino}/{ruta_relativa}"
            cliente.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": key_staging},
                Key=key_destino,
            )

        logger.info(
            "Parquet escrito en s3://%s/%s/fecha=%s/ (%d archivo(s)).",
            bucket,
            prefijo_destino,
            fecha,
            len(claves_staging),
        )
        return f"{prefijo_destino}/fecha={fecha}/"
    except Exception:
        logger.error(
            "Fallo al escribir el Parquet; se limpia el staging para no dejar "
            "particiones parciales en processed/."
        )
        raise
    finally:
        # Limpieza del staging en S3 (tanto en éxito como en fallo) y del temporal local.
        _eliminar_claves(cliente, bucket, claves_staging)
        shutil.rmtree(directorio_local, ignore_errors=True)


def _listar_archivos(directorio: str) -> list[str]:
    """Devuelve las rutas absolutas de todos los archivos bajo ``directorio``.

    Por qué: ``write_to_dataset`` genera subcarpetas de partición; se necesitan todas las
    rutas de archivo para subirlas a S3 preservando la estructura de particiones.
    """
    rutas: list[str] = []
    for raiz, _carpetas, archivos in os.walk(directorio):
        for nombre in archivos:
            rutas.append(os.path.join(raiz, nombre))
    return rutas


def _eliminar_claves(s3_client, bucket: str, claves: list[str]) -> None:
    """Elimina las claves de staging en S3 de forma best-effort.

    Por qué: el staging es temporal; tras promover los datos (o ante un fallo) no debe
    quedar basura en ``processed/_staging/``. Los errores de borrado se registran pero no
    interrumpen el flujo principal.
    """
    for clave in claves:
        try:
            s3_client.delete_object(Bucket=bucket, Key=clave)
        except Exception as error:  # pragma: no cover - limpieza best-effort
            logger.warning("No se pudo borrar el objeto de staging %s: %s", clave, error)


def main() -> None:
    """Punto de entrada del Glue Job: orquesta lectura, transformación y escritura.

    Lee los argumentos ``--bucket`` y ``--key`` con ``getResolvedOptions``, valida que la
    key sea un ``.csv``, deriva la fecha de partición del nombre del archivo, lee el CSV,
    lo transforma y lo escribe de forma atómica en ``processed/``. Cualquier error se
    propaga para que el job termine con estado de fallo sin dejar datos parciales
    (Requisitos 3.2, 3.3, 3.4, 3.5).
    """
    if getResolvedOptions is None:  # pragma: no cover - solo fuera del runtime de Glue
        raise RuntimeError(
            "getResolvedOptions no está disponible: este script debe ejecutarse como "
            "Glue Python Shell job."
        )

    args = getResolvedOptions(sys.argv, ["bucket", "key"])
    bucket, key = args["bucket"], args["key"]
    logger.info("Iniciando transformación de s3://%s/%s", bucket, key)

    # Validación temprana: sin .csv no se procesa (Requisito 3.4).
    if not key.endswith(".csv"):
        raise ValueError(f"La key '{key}' no apunta a un archivo .csv.")

    fecha = extraer_fecha(key)
    df = leer_csv(bucket, key)
    df_transformado = transformar(df, fecha)

    run_id = uuid.uuid4().hex
    destino = escribir_parquet_atomico(df_transformado, bucket, fecha, run_id)
    logger.info("Transformación completada. Datos en s3://%s/%s", bucket, destino)


if __name__ == "__main__":  # pragma: no cover
    main()
