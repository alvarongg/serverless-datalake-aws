"""Test de propiedad (Hypothesis) para la transformación CSV→Parquet del Glue Job.

Cubre la **Property 3** del diseño: la transformación preserva los datos y particiona
por la fecha de origen. Se ejercita la lógica pura/local de ``glue_src/transform_job.py``
(``transformar`` + ``escribir_dataset_parquet_local``), sin S3 ni boto3: se generan
DataFrames de ventas aleatorios, se transforman, se escriben como dataset Parquet
particionado en un directorio temporal y se vuelven a leer con descubrimiento de
particiones para comprobar el round-trip.

El test ejecuta un mínimo de 100 ejemplos (``@settings(max_examples=100)``) e implementa
una única propiedad de corrección.
"""

import shutil
import string
import tempfile

import pandas as pd
import pyarrow.parquet as pq
from hypothesis import given, settings
from hypothesis import strategies as st
from pandas.testing import assert_frame_equal

from transform_job import (
    COLUMNA_PARTICION,
    escribir_dataset_parquet_local,
    transformar,
)

# Columnas de datos del CSV de ventas (sin contar la partición ``fecha``, que la fija
# ``transformar`` a partir del nombre del archivo de origen). Coinciden con el esquema
# lógico del diseño: sucursal, producto, cantidad, monto.
_COLUMNAS_DATOS = ["sucursal", "producto", "cantidad", "monto"]

# Alfabeto para los textos (sucursal/producto): ASCII imprimible para que el round-trip
# por Parquet sea exacto y la comparación no dependa de normalizaciones de codificación.
_ALFABETO_TEXTO = string.ascii_letters + string.digits + " -_."


@st.composite
def ventas_dataframes(draw):
    """Genera un DataFrame de ventas aleatorio (filas, valores) con el esquema fijo.

    Por qué: el round-trip debe verificarse para cualquier contenido de ventas válido.
    Se varían el número de filas y los valores de cada columna dentro de los rangos del
    diseño (``cantidad`` entero 1–1000, ``monto`` decimal 0.01–999999.99). Se incluye una
    columna ``fecha`` de origen (con fechas arbitrarias) para reflejar el CSV real; la
    transformación la sobrescribe con la fecha de partición.
    """
    n_filas = draw(st.integers(min_value=1, max_value=30))

    def columna_texto():
        return draw(
            st.lists(
                st.text(alphabet=_ALFABETO_TEXTO, min_size=0, max_size=15),
                min_size=n_filas,
                max_size=n_filas,
            )
        )

    sucursales = columna_texto()
    productos = columna_texto()
    cantidades = draw(
        st.lists(st.integers(min_value=1, max_value=1000), min_size=n_filas, max_size=n_filas)
    )
    montos = draw(
        st.lists(
            st.floats(
                min_value=0.01,
                max_value=999999.99,
                allow_nan=False,
                allow_infinity=False,
            ),
            min_size=n_filas,
            max_size=n_filas,
        )
    )
    # Fechas arbitrarias en el origen: la transformación las reemplaza por la partición.
    fechas_origen = draw(
        st.lists(
            st.dates().map(lambda d: d.strftime("%Y-%m-%d")),
            min_size=n_filas,
            max_size=n_filas,
        )
    )

    return pd.DataFrame(
        {
            "fecha": fechas_origen,
            "sucursal": sucursales,
            "producto": productos,
            "cantidad": cantidades,
            "monto": montos,
        }
    )


def _normalizar(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza un DataFrame para comparar ignorando orden de filas, columnas y dtype.

    Por qué: ``write_to_dataset`` no garantiza el orden de filas ni de columnas al releer,
    y la columna de partición puede volver como categoría/string. Se castea ``fecha`` a
    string, se ordenan columnas y filas de forma estable y se reinicia el índice para que
    la comparación sea por contenido, no por disposición.
    """
    df = df.copy()
    df[COLUMNA_PARTICION] = df[COLUMNA_PARTICION].astype(str)
    columnas = sorted(df.columns)
    df = df[columnas]
    df = df.sort_values(by=columnas, kind="stable").reset_index(drop=True)
    return df


# Feature: serverless-datalake-aws, Property 3: La transformación preserva los datos y particiona por la fecha de origen
@settings(max_examples=100, deadline=None)
@given(df_origen=ventas_dataframes(), fecha_particion=st.dates().map(lambda d: d.strftime("%Y-%m-%d")))
def test_transformacion_preserva_datos_y_particiona_por_fecha(df_origen, fecha_particion):
    """Para cualquier CSV de ventas válido, transformar a Parquet y volver a leerlo
    preserva todas las filas y columnas y deja los registros bajo ``fecha=<AAAA-MM-DD>``."""
    directorio = tempfile.mkdtemp(prefix="test_parquet_")
    try:
        # 1. Transformar: fija la columna de partición ``fecha`` a la fecha de origen.
        df_transformado = transformar(df_origen, fecha_particion)

        # 2. Escribir el dataset Parquet particionado en disco local (sin S3).
        escribir_dataset_parquet_local(df_transformado, directorio)

        # 3. Volver a leer con descubrimiento de particiones (partición Hive).
        df_leido = pq.read_table(directorio).to_pandas()

        # --- Aserción A: se preservan todas las columnas del CSV de origen.
        assert set(df_leido.columns) == set(df_origen.columns)

        # --- Aserción B: se preservan todas las filas (mismo conteo).
        assert len(df_leido) == len(df_origen)

        # --- Aserción C: los registros quedan bajo la única partición fecha=<AAAA-MM-DD>.
        import os

        particiones = sorted(os.listdir(directorio))
        assert particiones == [f"{COLUMNA_PARTICION}={fecha_particion}"]
        assert set(df_leido[COLUMNA_PARTICION].astype(str).unique()) == {fecha_particion}

        # --- Aserción D: el contenido releído es idéntico al transformado (round-trip).
        assert_frame_equal(
            _normalizar(df_transformado),
            _normalizar(df_leido),
            check_dtype=False,
        )
    finally:
        shutil.rmtree(directorio, ignore_errors=True)
