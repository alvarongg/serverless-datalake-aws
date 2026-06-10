"""Tests de propiedad (Hypothesis) para la resolución de cuenta/región.

Cubre la **Property 5** del diseño: la función pura ``resolve_environment`` de
``app.py`` resuelve cuenta y región siguiendo la precedencia definida (environment de
CDK → parámetro por defecto configurable) sin recurrir a ningún valor literal
hardcodeado; y, cuando no puede determinar la cuenta o la región, falla con
``EnvironmentResolutionError`` indicando cuál de los dos valores falta, sin crear
recursos.

Sobre la importación del módulo: ``app.py`` vive en la raíz del repositorio (no es un
paquete). El ``conftest.py`` de la suite agrega la raíz del proyecto al ``sys.path``
antes de importar. Importar ``app`` no ejecuta ``main()`` (está protegido por
``__main__``), por lo que no se sintetiza ni se crea ningún recurso al importar.

Cada test ejecuta un mínimo de 100 ejemplos (``@settings(max_examples=100)``) e
implementa una única propiedad de corrección.
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from app import (
    DEFAULT_REGION_CONTEXT_KEY,
    EnvironmentResolutionError,
    ResolvedEnvironment,
    resolve_environment,
)

# Clave de contexto que NO debe consultarse para resolver la región (sirve para
# verificar que ``context_lookup`` solo se usa con la clave esperada).
_CLAVE_CONTEXTO = DEFAULT_REGION_CONTEXT_KEY

# Valores "presentes": cadenas no vacías y con contenido tras hacer ``strip``. Se
# incluyen valores con espacios alrededor para verificar que la resolución normaliza
# con ``strip`` antes de devolverlos.
_VALORES_PRESENTES = st.sampled_from(
    [
        "123456789012",
        "us-east-1",
        "eu-west-1",
        "  sa-east-1  ",  # con espacios: deben recortarse pero cuenta como presente
        "ap-southeast-2",
        "987654321098",
    ]
)

# Valores "ausentes": ni clave en el dict, o cadena vacía/solo espacios (que la función
# trata como ausente según la precedencia).
_VALORES_AUSENTES = st.sampled_from([None, "", "   ", "\t", "\n  "])

# Un candidato puede estar presente o ausente, con igual oportunidad de cubrir ambos.
_CANDIDATO = st.one_of(_VALORES_PRESENTES, _VALORES_AUSENTES)


def _construir_env(cdk_account, cdk_region, datalake_region):
    """Construye el dict de environment omitiendo las claves cuyo valor es ``None``.

    Por qué: distinguir entre "la variable no existe" (clave ausente) y "la variable
    existe pero está vacía" (cadena vacía/espacios). Ambos casos deben tratarse como
    ausentes por la función, y este helper permite ejercitar las dos formas.
    """
    env = {}
    if cdk_account is not None:
        env["CDK_DEFAULT_ACCOUNT"] = cdk_account
    if cdk_region is not None:
        env["CDK_DEFAULT_REGION"] = cdk_region
    if datalake_region is not None:
        env["DATALAKE_REGION"] = datalake_region
    return env


def _crear_context_lookup(valor_contexto):
    """Crea un ``context_lookup`` que devuelve ``valor_contexto`` solo para la clave
    esperada y ``None`` para cualquier otra.

    Por qué: replica el comportamiento de ``app.node.try_get_context``, que devuelve
    ``None`` cuando la clave de contexto no está definida.
    """

    def context_lookup(clave):
        return valor_contexto if clave == _CLAVE_CONTEXTO else None

    return context_lookup


def _es_presente(valor):
    """Indica si un valor cuenta como "presente" según el criterio de la función:
    cadena no vacía tras ``strip``."""
    return isinstance(valor, str) and bool(valor.strip())


def _primera_region_esperada(cdk_region, datalake_region, contexto):
    """Devuelve la región que debería ganar por precedencia, ya normalizada con
    ``strip``, o ``None`` si ninguna fuente está presente.

    La precedencia es: ``CDK_DEFAULT_REGION`` → ``DATALAKE_REGION`` → contexto.
    """
    for valor in (cdk_region, datalake_region, contexto):
        if _es_presente(valor):
            return valor.strip()
    return None


# Feature: serverless-datalake-aws, Property 5: Resolución de cuenta/región según
# precedencia y sin valores hardcodeados
@settings(max_examples=100)
@given(
    cdk_account=_CANDIDATO,
    cdk_region=_CANDIDATO,
    datalake_region=_CANDIDATO,
    contexto=_CANDIDATO,
)
def test_resolve_environment_precedencia_y_fallo(
    cdk_account, cdk_region, datalake_region, contexto
):
    """Para cualquier combinación de variables de entorno y contexto de CDK:

    - Si la cuenta está presente y al menos una fuente de región está presente,
      ``resolve_environment`` devuelve un ``ResolvedEnvironment`` cuya región es la
      PRIMERA fuente no vacía por precedencia (``CDK_DEFAULT_REGION`` →
      ``DATALAKE_REGION`` → contexto) y cuya cuenta es ``CDK_DEFAULT_ACCOUNT``
      (recortada). No aparece ningún literal de cuenta/región hardcodeado.
    - Si la cuenta está ausente o todas las fuentes de región están ausentes, lanza
      ``EnvironmentResolutionError`` cuyo mensaje menciona "cuenta" cuando falta la
      cuenta y "región" cuando falta la región, sin crear recursos.
    """
    env = _construir_env(cdk_account, cdk_region, datalake_region)
    context_lookup = _crear_context_lookup(contexto)

    cuenta_presente = _es_presente(cdk_account)
    region_esperada = _primera_region_esperada(cdk_region, datalake_region, contexto)

    if cuenta_presente and region_esperada is not None:
        resuelto = resolve_environment(env, context_lookup)
        assert isinstance(resuelto, ResolvedEnvironment)
        # La cuenta proviene exclusivamente de CDK_DEFAULT_ACCOUNT, recortada.
        assert resuelto.account == cdk_account.strip()
        # La región es la primera fuente no vacía según la precedencia.
        assert resuelto.region == region_esperada
    else:
        with pytest.raises(EnvironmentResolutionError) as excinfo:
            resolve_environment(env, context_lookup)
        mensaje = str(excinfo.value).lower()
        # El mensaje debe señalar con precisión cuál de los dos valores falta.
        if not cuenta_presente:
            assert "cuenta" in mensaje
        if region_esperada is None:
            assert "región" in mensaje or "region" in mensaje
