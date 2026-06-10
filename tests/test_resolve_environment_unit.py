"""Unit tests (basados en ejemplos) de la resolución de cuenta/región.

Complementan el test de propiedad de la **Property 5** (``test_resolve_environment_properties.py``)
cubriendo de forma concreta el **Requisito 11.3**: cuando no es posible determinar la
cuenta ni la región, ``resolve_environment`` debe fallar con
``EnvironmentResolutionError`` indicando con precisión cuál de los dos valores falta,
sin crear ningún recurso.

Por qué ejemplos además de la propiedad: las aserciones basadas en ejemplos fijan el
contrato observable del mensaje de error (qué término aparece y qué término NO debe
aparecer según el valor faltante), algo que el test de propiedad solo comprueba de
forma genérica. Importar ``app`` no ejecuta ``main()`` (está protegido por
``__main__``), de modo que estos tests no sintetizan el stack ni crean recursos; el
``conftest.py`` de la suite agrega la raíz del repositorio al ``sys.path``.
"""

import pytest

from app import (
    EnvironmentResolutionError,
    ResolvedEnvironment,
    resolve_environment,
)


def _sin_contexto(_clave):
    """``context_lookup`` que simula la ausencia total de contexto de CDK.

    Por qué: ``app.node.try_get_context`` devuelve ``None`` cuando la clave no está
    definida; esta función replica ese comportamiento para cualquier clave.
    """
    return None


def test_falta_cuenta_y_region_menciona_ambas():
    """Sin cuenta ni región en ninguna fuente: error que menciona AMBOS valores.

    Cubre el caso límite del Requisito 11.3 donde no hay nada que resolver: el
    mensaje debe señalar tanto la cuenta como la región como faltantes.
    """
    with pytest.raises(EnvironmentResolutionError) as excinfo:
        resolve_environment({}, _sin_contexto)

    mensaje = str(excinfo.value).lower()
    assert "cuenta" in mensaje
    assert "región" in mensaje or "region" in mensaje


def test_solo_cuenta_presente_menciona_region_y_no_cuenta():
    """Cuenta presente pero región ausente: el error menciona la región y NO afirma
    que falte la cuenta.

    Verifica que el mensaje es preciso: como ``CDK_DEFAULT_ACCOUNT`` está definido,
    la cuenta no debe figurar entre los valores faltantes.
    """
    env = {"CDK_DEFAULT_ACCOUNT": "123456789012"}

    with pytest.raises(EnvironmentResolutionError) as excinfo:
        resolve_environment(env, _sin_contexto)

    mensaje = str(excinfo.value).lower()
    assert "región" in mensaje or "region" in mensaje
    # La cuenta está presente: no debe reportarse como faltante.
    assert "cuenta" not in mensaje


def test_solo_region_presente_menciona_cuenta():
    """Región presente pero cuenta ausente: el error menciona la cuenta.

    Como ``CDK_DEFAULT_REGION`` está definido, solo la cuenta debe figurar como
    valor faltante en el mensaje.
    """
    env = {"CDK_DEFAULT_REGION": "us-east-1"}

    with pytest.raises(EnvironmentResolutionError) as excinfo:
        resolve_environment(env, _sin_contexto)

    mensaje = str(excinfo.value).lower()
    assert "cuenta" in mensaje


def test_ambos_presentes_devuelve_environment_resuelto():
    """Sanity check: con cuenta y región presentes no se lanza excepción y se
    devuelve un ``ResolvedEnvironment`` con los valores esperados."""
    env = {
        "CDK_DEFAULT_ACCOUNT": "123456789012",
        "CDK_DEFAULT_REGION": "eu-west-1",
    }

    resuelto = resolve_environment(env, _sin_contexto)

    assert isinstance(resuelto, ResolvedEnvironment)
    assert resuelto.account == "123456789012"
    assert resuelto.region == "eu-west-1"
