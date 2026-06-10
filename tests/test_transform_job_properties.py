"""Tests de propiedad (Hypothesis) para la lógica pura del Glue Job.

Cubre la **Property 2** del diseño: el round-trip de extracción de fecha del nombre
de archivo de ``extraer_fecha`` (``glue_src/transform_job.py``). Se usan generadores
inteligentes que construyen nombres de archivo con una fecha ``AAAA-MM-DD`` válida
embebida y, por separado, cadenas que no contienen ninguna fecha válida en ese formato.

Cada test ejecuta un mínimo de 100 ejemplos (``@settings(max_examples=100)``) e
implementa una única propiedad de corrección.
"""

import string
from datetime import date

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from transform_job import extraer_fecha

# Alfabeto para los afijos (texto antes/después de la fecha). Deliberadamente SIN
# dígitos: así se garantiza que la única coincidencia posible del patrón
# AAAA-MM-DD en el nombre generado sea la fecha que insertamos a propósito, evitando
# fechas espurias introducidas por el texto aleatorio.
_ALFABETO_AFIJO = string.ascii_letters + "_/.-"


@st.composite
def nombres_con_fecha_valida(draw):
    """Genera ``(fecha_str, nombre_archivo)`` con una fecha AAAA-MM-DD válida embebida.

    Por qué: para verificar el round-trip necesitamos conocer la fecha exacta que
    insertamos. Se acota el año a 4 dígitos (>= 1000) para que coincida con el patrón
    ``\\d{4}`` y los afijos no contienen dígitos, de modo que la fecha insertada sea la
    única coincidencia del patrón en el nombre.
    """
    fecha = draw(st.dates(min_value=date(1000, 1, 1), max_value=date(9999, 12, 31)))
    fecha_str = fecha.strftime("%Y-%m-%d")
    prefijo = draw(st.text(alphabet=_ALFABETO_AFIJO, max_size=20))
    sufijo = draw(st.text(alphabet=_ALFABETO_AFIJO, max_size=20))
    nombre = f"{prefijo}{fecha_str}{sufijo}.csv"
    return fecha_str, nombre


@st.composite
def nombres_sin_fecha_valida(draw):
    """Genera nombres de archivo que NO contienen una fecha AAAA-MM-DD válida.

    Tres familias, todas casos legítimos de "sin fecha válida":
    1. Texto sin dígitos: imposible que contenga el patrón.
    2. La cadena vacía.
    3. Cadenas que coinciden con el patrón pero con un mes inválido (13-99), es decir,
       sintácticamente parecidas a una fecha pero que no son una fecha de calendario.
    """
    sin_digitos = st.text(alphabet=_ALFABETO_AFIJO, max_size=40)

    @st.composite
    def fecha_calendario_invalida(sub_draw):
        anio = sub_draw(st.integers(min_value=1000, max_value=9999))
        mes = sub_draw(st.integers(min_value=13, max_value=99))  # mes inexistente
        dia = sub_draw(st.integers(min_value=1, max_value=99))
        prefijo = sub_draw(st.text(alphabet=_ALFABETO_AFIJO, max_size=10))
        return f"{prefijo}{anio:04d}-{mes:02d}-{dia:02d}.csv"

    return draw(st.one_of(sin_digitos, st.just(""), fecha_calendario_invalida()))


# Feature: serverless-datalake-aws, Property 2: Round-trip de extracción de fecha del nombre de archivo
@settings(max_examples=100)
@given(datos=nombres_con_fecha_valida())
def test_extraer_fecha_recupera_la_fecha_embebida(datos):
    """Para cualquier fecha AAAA-MM-DD válida embebida en un nombre de archivo,
    ``extraer_fecha`` recupera exactamente esa fecha (round-trip)."""
    fecha_str, nombre = datos
    assert extraer_fecha(nombre) == fecha_str


# Feature: serverless-datalake-aws, Property 2: Round-trip de extracción de fecha del nombre de archivo
@settings(max_examples=100)
@given(nombre=nombres_sin_fecha_valida())
def test_extraer_fecha_falla_sin_fecha_valida(nombre):
    """Para cualquier nombre de archivo sin una fecha AAAA-MM-DD válida,
    ``extraer_fecha`` señala un error en lugar de inventar una fecha."""
    with pytest.raises(ValueError):
        extraer_fecha(nombre)
