"""Tests de propiedad (Hypothesis) para la decisión de filtrado de la Trigger_Lambda.

Cubre la **Property 1** del diseño: la función pura ``debe_procesar`` de
``lambda_src/trigger_pipeline.py`` decide iniciar el Glue Job **si y solo si** la key
del objeto S3 empieza con el prefijo ``raw/`` y termina con el sufijo ``.csv``.

Sobre la importación del módulo: ``trigger_pipeline`` crea un ``boto3.client("glue")``
en tiempo de importación. El ``conftest.py`` de la suite agrega ``lambda_src/`` al
``sys.path`` y fija una región AWS por defecto antes de importar, de modo que el
``import`` funcione en un entorno sin credenciales/perfil AWS (p. ej. en CI) sin
realizar ninguna llamada real a AWS.

Cada test ejecuta un mínimo de 100 ejemplos (``@settings(max_examples=100)``) e
implementa una única propiedad de corrección.
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from trigger_pipeline import debe_procesar

# Prefijos representativos del data lake: la zona raw/ (única que debe disparar el job)
# y las demás zonas/valores que NO deben dispararlo (defensa en profundidad).
_PREFIJOS = st.sampled_from(
    [
        "raw/",
        "raw/ventas/",
        "processed/",
        "processed/ventas/",
        "curated/",
        "athena-results/",
        "",  # sin prefijo de zona
        "logs/",
        "RAW/",  # mayúsculas: NO coincide con el prefijo exacto "raw/"
    ]
)

# Sufijos representativos: .csv (el único válido) y otros que no deben disparar el job.
_SUFIJOS = st.sampled_from(
    [
        ".csv",
        ".parquet",
        ".CSV",  # mayúsculas: NO coincide con el sufijo exacto ".csv"
        ".txt",
        ".csv.gz",  # termina en .gz, no en .csv
        "",  # sin sufijo de extensión
    ]
)


@st.composite
def keys_de_s3(draw):
    """Genera keys de S3 combinando un prefijo de zona, un cuerpo y un sufijo.

    Por qué este diseño: cubrir deliberadamente todas las combinaciones de zona
    (``raw/`` vs. el resto) y de extensión (``.csv`` vs. el resto), incluyendo casos
    límite con mayúsculas y dobles extensiones, para ejercitar el ``si y solo si`` de
    la decisión de filtrado en ambos sentidos.
    """
    prefijo = draw(_PREFIJOS)
    # Cuerpo del nombre del archivo: texto arbitrario (puede ir vacío) que puede
    # contener cualquier carácter salvo barras adicionales que confundan la lectura.
    cuerpo = draw(st.text(max_size=30))
    sufijo = draw(_SUFIJOS)
    return f"{prefijo}{cuerpo}{sufijo}"


# Feature: serverless-datalake-aws, Property 1: Decisión de filtrado de la Lambda
@settings(max_examples=100)
@given(key=keys_de_s3())
def test_debe_procesar_es_iff_raw_y_csv(key):
    """Para cualquier key de S3, ``debe_procesar`` devuelve True si y solo si la key
    empieza con ``raw/`` y termina con ``.csv``; cualquier otra key (incluidas las de
    processed/, curated/ y las de raw/ sin sufijo .csv) devuelve False."""
    esperado = key.startswith("raw/") and key.endswith(".csv")
    assert debe_procesar(key) == esperado


# Feature: serverless-datalake-aws, Property 1: Decisión de filtrado de la Lambda
@settings(max_examples=100)
@given(key=st.text(max_size=80))
def test_debe_procesar_iff_para_texto_arbitrario(key):
    """La equivalencia (iff) se mantiene también para keys de texto completamente
    arbitrario, no solo para las combinaciones de zona/extensión predefinidas."""
    esperado = key.startswith("raw/") and key.endswith(".csv")
    assert debe_procesar(key) == esperado
