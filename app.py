#!/usr/bin/env python3
"""Punto de entrada de la aplicación CDK del data lake serverless.

Por qué este archivo es el único que conoce cuenta y región: el stack
(`DataLakeStack`) debe permanecer libre de literales de cuenta/región para que
el mismo código se despliegue en cualquier cuenta sin editarse (Requisito 11).
Aquí resolvemos ese environment a partir del entorno de ejecución de CDK y se lo
inyectamos al stack mediante el parámetro `env`.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import NamedTuple

import aws_cdk as cdk

from datalake.datalake_stack import DataLakeStack

# Clave de contexto de CDK usada como región por defecto configurable
# (definida en cdk.json y sobreescribible con `-c datalake:defaultRegion=...`).
DEFAULT_REGION_CONTEXT_KEY = "datalake:defaultRegion"


class ResolvedEnvironment(NamedTuple):
    """Cuenta y región ya resueltas para desplegar el stack.

    Se modela como tupla nombrada inmutable porque, una vez resuelto el
    environment, ninguna parte del programa debería mutarlo.
    """

    account: str
    region: str


class EnvironmentResolutionError(RuntimeError):
    """Error de resolución de environment cuando falta cuenta o región.

    Se usa una excepción propia para que el mensaje indique con claridad cuál
    de los dos valores no se pudo determinar (Requisito 11.3) y para poder
    distinguirla en los tests.
    """


def resolve_environment(
    env: Mapping[str, str],
    context_lookup: Callable[[str], object | None],
) -> ResolvedEnvironment:
    """Resuelve cuenta y región sin recurrir a literales hardcodeados.

    Por qué es una función pura (sin leer `os.environ` ni el `App` directamente):
    así puede probarse con property-based testing pasándole distintas
    combinaciones de entorno y contexto (Property 5).

    Precedencia de resolución (Requisito 11.1, 11.2):

    - Cuenta: variable `CDK_DEFAULT_ACCOUNT` inyectada por la CLI de CDK desde el
      perfil de AWS activo.
    - Región: primero `CDK_DEFAULT_REGION`; si no está, la variable propia
      `DATALAKE_REGION`; y por último el contexto de CDK
      (`datalake:defaultRegion`) como valor por defecto configurable fuera del
      código del stack.

    Args:
        env: Mapeo de variables de entorno (normalmente `os.environ`).
        context_lookup: Función que devuelve el valor de una clave de contexto de
            CDK, o `None` si no existe (normalmente `app.node.try_get_context`).

    Returns:
        El environment resuelto con cuenta y región no vacías.

    Raises:
        EnvironmentResolutionError: Si no se puede determinar la cuenta o la
            región; el mensaje indica cuál de los dos falta.
    """
    # La cuenta solo puede provenir del environment de CDK: nunca se hardcodea.
    account = _primer_valor_no_vacio(env.get("CDK_DEFAULT_ACCOUNT"))

    # La región sigue la precedencia: entorno de CDK -> variable propia -> contexto.
    region = _primer_valor_no_vacio(
        env.get("CDK_DEFAULT_REGION"),
        env.get("DATALAKE_REGION"),
        context_lookup(DEFAULT_REGION_CONTEXT_KEY),
    )

    # Reportar con precisión cuál de los dos valores falta (Requisito 11.3).
    faltantes = []
    if account is None:
        faltantes.append("cuenta (CDK_DEFAULT_ACCOUNT)")
    if region is None:
        faltantes.append(
            "región (CDK_DEFAULT_REGION, DATALAKE_REGION o contexto "
            f"'{DEFAULT_REGION_CONTEXT_KEY}')"
        )

    if faltantes:
        raise EnvironmentResolutionError(
            "No se pudo resolver el environment de despliegue. "
            "Falta: " + " y ".join(faltantes) + "."
        )

    return ResolvedEnvironment(account=account, region=region)


def _primer_valor_no_vacio(*candidatos: object | None) -> str | None:
    """Devuelve el primer candidato que sea una cadena no vacía (sin espacios).

    Por qué: unifica el criterio de "valor presente" para cuenta y región, de
    modo que un string vacío o solo con espacios cuente como ausente y se siga
    intentando con el siguiente candidato de la precedencia.
    """
    for candidato in candidatos:
        if isinstance(candidato, str) and candidato.strip():
            return candidato.strip()
    return None


def main() -> cdk.App:
    """Construye la `App` de CDK e instancia `DataLakeStack` con el env resuelto.

    Se separa de la ejecución a nivel de módulo para poder importar `main` en los
    tests sin provocar efectos colaterales no deseados.
    """
    app = cdk.App()

    resolved = resolve_environment(os.environ, app.node.try_get_context)

    DataLakeStack(
        app,
        "DataLakeStack",
        env=cdk.Environment(account=resolved.account, region=resolved.region),
        description="Data Lake serverless en AWS definido con CDK v2 (Python).",
    )

    return app


if __name__ == "__main__":
    main().synth()
