"""Definición del único stack del data lake serverless: `DataLakeStack`.

Por qué un solo stack: el proyecto es educativo y mantiene toda la
infraestructura junta para que el lector siga el flujo de principio a fin sin
saltar entre stacks. Este archivo arranca como un esqueleto vacío (sintetiza sin
recursos) y se irá completando tarea a tarea según el plan del spec.

Importante: este archivo nunca contiene literales de cuenta ni región; esos
valores se resuelven en `app.py` y se inyectan vía el parámetro `env`
(Requisito 11.1).
"""

from __future__ import annotations

from aws_cdk import Stack
from constructs import Construct


class DataLakeStack(Stack):
    """Stack que agrupa toda la infraestructura del data lake serverless.

    Por ahora es un esqueleto sin recursos: existe para que `app.py` pueda
    instanciarlo y `cdk synth` produzca un template válido. Las siguientes tareas
    del plan irán añadiendo el bucket S3, los roles IAM, el Glue Job, el crawler,
    la Lambda, el workgroup de Athena, Lake Formation y los outputs.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        """Inicializa el stack delegando la configuración base en `Stack`.

        Args:
            scope: Construct padre (normalmente la `App` de CDK).
            construct_id: Identificador lógico del stack dentro del árbol de CDK.
            **kwargs: Argumentos de `Stack`, incluido `env` con cuenta y región
                ya resueltas en `app.py`.
        """
        super().__init__(scope, construct_id, **kwargs)

        # Los recursos se añadirán en las próximas tareas del plan de implementación.
