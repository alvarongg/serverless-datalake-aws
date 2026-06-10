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

from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3
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

        # === Data_Lake_Bucket (S3) ===
        # Base de toda la arquitectura: el bucket único que almacena las tres
        # zonas del data lake. El resto de los recursos (Lambda, Glue, Athena,
        # Lake Formation) depende de él, por eso se crea primero.
        #
        # Las "zonas" del data lake son PREFIJOS LÓGICOS dentro de este único
        # bucket, NO recursos de AWS independientes:
        #   - raw/             -> CSV crudos que suben las sucursales.
        #   - processed/       -> Parquet particionado por fecha (salida del Glue Job).
        #   - curated/         -> datos refinados de consumo final (reservado).
        #   - athena-results/  -> resultados de las consultas de Athena.
        # No se crean explícitamente: se materializan al escribir el primer
        # objeto bajo cada prefijo. Mantenerlos como convención evita gestionar
        # múltiples buckets y deja el acoplamiento entre servicios en la simple
        # convención de ubicación.
        self.data_lake_bucket = s3.Bucket(
            self,
            "DataLakeBucket",
            # Cifrado en reposo SSE-S3: todo objeto se cifra automáticamente,
            # incluso si se sube sin cabecera de cifrado (Requisito 1.2, 1.3).
            encryption=s3.BucketEncryption.S3_MANAGED,
            # Bloqueo total de acceso público: activa las cuatro flags
            # (BlockPublicAcls, IgnorePublicAcls, BlockPublicPolicy y
            # RestrictPublicBuckets) en true; ninguna política o ACL podrá
            # abrir el bucket al público (Requisito 1.4, 1.5).
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            # Versionado activado: protege ante sobrescrituras y borrados
            # accidentales conservando versiones previas (Requisito 1.6).
            versioned=True,
            # Proyecto educativo: el bucket debe poder destruirse limpio. Al
            # ejecutar `cdk destroy`, RemovalPolicy.DESTROY elimina el bucket y
            # auto_delete_objects borra antes todos los objetos y versiones para
            # que no queden recursos huérfanos (Requisito 1.7, 1.8).
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            # Buena práctica de seguridad adicional: rechaza cualquier petición
            # que no use TLS (deniega tráfico no cifrado en tránsito).
            enforce_ssl=True,
        )
