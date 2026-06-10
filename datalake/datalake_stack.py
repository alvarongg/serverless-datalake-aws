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

import os

from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_assets as s3_assets
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

        # === Glue_Data_Catalog: base de datos `datalake_db` ===
        # Base de datos del catálogo de Glue donde el crawler registrará las
        # tablas inferidas a partir de los datos Parquet de la zona processed/.
        # Es el punto de entrada para que Athena consulte el data lake por SQL.
        #
        # Se usa el recurso de bajo nivel `CfnDatabase` porque ofrece control
        # explícito sobre el nombre físico (`datalake_db`), que otras tareas
        # (crawler, permisos de Lake Formation y outputs) necesitan referenciar
        # de forma estable. El `catalog_id` es el ID de la cuenta (cada cuenta
        # tiene un único Data Catalog por región). Se guarda la referencia en
        # `self.datalake_database` para reutilizarla en tareas posteriores
        # (Requisito 4.1).
        self.datalake_database = glue.CfnDatabase(
            self,
            "DatalakeDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="datalake_db",
            ),
        )

        # === Glue_Transform_Job (Python Shell) y su rol IAM ===
        # El job lee un CSV de la zona raw/, lo transforma a Parquet particionado
        # por fecha y lo escribe en processed/. El script vive en glue_src/ como
        # ASSET separado de la infraestructura (Requisito 3.1): nunca se mezcla la
        # lógica de runtime con la definición de recursos.
        #
        # El job y su rol se definen juntos porque son recursos estrechamente
        # relacionados (el job necesita el ARN del rol para asumirlo).

        # --- Asset: subir el script del Glue Job a S3 ---
        # CDK empaqueta glue_src/transform_job.py y lo sube al bucket de assets
        # del entorno (bootstrap). El job referencia su ubicación en S3 vía la
        # propiedad `script_location` del comando.
        transform_job_script = s3_assets.Asset(
            self,
            "TransformJobScript",
            path=os.path.join(
                os.path.dirname(__file__), "..", "glue_src", "transform_job.py"
            ),
        )

        # --- Glue Job Role: mínimo privilegio (Requisito 7.2, 7.3, 7.5) ---
        # El rol que asume el Glue Job. Se crea con principal de servicio de Glue
        # y SIN políticas administradas con comodines (la AWSGlueServiceRole usa
        # Resource "*", por eso no se adjunta): cada permiso se concede de forma
        # explícita con ARNs concretos.
        self.transform_job_role = iam.Role(
            self,
            "TransformJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description=(
                "Rol de minimo privilegio del Glue Transform Job: lee raw/, "
                "escribe processed/, sin acceso a curated/."
            ),
        )

        # Lectura restringida a la zona raw/ (Requisito 7.2). ARN concreto del
        # objeto bajo el prefijo raw/, nunca "*". No se concede escritura sobre
        # raw/ (Requisito 7.5).
        self.transform_job_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadRawObjects",
                actions=["s3:GetObject"],
                resources=[self.data_lake_bucket.arn_for_objects("raw/*")],
            )
        )

        # Escritura (y borrado para la limpieza del staging atómico) restringida a
        # la zona processed/ (Requisito 7.2, 7.5). El job escribe Parquet en
        # processed/ventas/ y usa processed/_staging/ como zona temporal.
        self.transform_job_role.add_to_policy(
            iam.PolicyStatement(
                sid="WriteProcessedObjects",
                actions=["s3:PutObject", "s3:DeleteObject"],
                resources=[self.data_lake_bucket.arn_for_objects("processed/*")],
            )
        )

        # Listado del bucket acotado por condición de prefijo a raw/ y processed/.
        # El Resource es el ARN concreto del bucket (no "*") y la condición
        # `s3:prefix` evita listar curated/ u otras zonas.
        self.transform_job_role.add_to_policy(
            iam.PolicyStatement(
                sid="ListBucketScopedToZones",
                actions=["s3:ListBucket"],
                resources=[self.data_lake_bucket.bucket_arn],
                conditions={
                    "StringLike": {"s3:prefix": ["raw/*", "processed/*"]}
                },
            )
        )

        # Logs de Glue en CloudWatch con ARN concreto del grupo de logs de Glue
        # (arn:aws:logs:<region>:<account>:log-group:/aws-glue/*), nunca "*".
        self.transform_job_role.add_to_policy(
            iam.PolicyStatement(
                sid="GlueCloudWatchLogs",
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=[
                    f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws-glue/*"
                ],
            )
        )

        # Permiso para que el job lea el script desde el bucket de assets. El
        # helper `grant_read` acota el permiso al objeto del asset (ARN concreto
        # del bucket de assets y del script), no a "*".
        transform_job_script.grant_read(self.transform_job_role)

        # --- CfnJob tipo Python Shell ---
        # Se elige Python Shell (no Spark): el escenario maneja un CSV diario y
        # pequeño; Python Shell con pandas/pyarrow es más simple, barato y legible
        # (ver design.md). El conjunto de librerías "analytics" provee pandas y
        # pyarrow en el entorno del job; requiere glue_version 3.0 (Python 3.9),
        # que es la versión máxima de Python soportada por Glue Python Shell.
        self.transform_job = glue.CfnJob(
            self,
            "TransformJob",
            role=self.transform_job_role.role_arn,
            glue_version="3.0",
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                python_version="3.9",
                script_location=transform_job_script.s3_object_url,
            ),
            # 1 DPU es suficiente para un CSV pequeño con el set analytics.
            max_capacity=1.0,
            default_arguments={
                # Habilita pandas/pyarrow en el entorno del Python Shell.
                "--library-set": "analytics",
            },
            description=(
                "Glue Python Shell job que transforma CSV de raw/ a Parquet "
                "particionado por fecha en processed/."
            ),
        )

        # Nombre del job para tareas posteriores (variable de entorno de la
        # Lambda, output del stack y predicado del Glue Trigger). Al no fijar un
        # nombre físico, se usa la referencia de CloudFormation, que para
        # AWS::Glue::Job resuelve al nombre del job.
        self.transform_job_name = self.transform_job.ref

        # === Glue_Crawler, su rol IAM y el Glue Trigger condicional ===
        # El crawler cataloga los datos Parquet de la zona processed/ en la base
        # de datos `datalake_db`, infiriendo esquema y particiones por fecha. Se
        # dispara automáticamente cuando el Glue Job termina con éxito (vía un
        # Glue Trigger condicional) y también puede iniciarse bajo demanda con
        # `aws glue start-crawler --name <crawler>` (Requisito 4.4, 4.5).
        #
        # El crawler, su rol y el trigger se definen juntos porque son recursos
        # estrechamente relacionados: el trigger encadena el job con el crawler y
        # el crawler necesita el ARN de su rol para asumirlo.

        # --- ARNs concretos del Glue Data Catalog (mínimo privilegio) ---
        # Se construyen a partir de región y cuenta resueltas por `app.py`; nunca
        # se usa Resource "*" (Requisito 7.3 / Property 6). El crawler solo puede
        # operar sobre el catálogo de la cuenta, la base `datalake_db` y sus
        # tablas, no sobre otras bases del catálogo.
        glue_catalog_arn = f"arn:aws:glue:{self.region}:{self.account}:catalog"
        glue_database_arn = (
            f"arn:aws:glue:{self.region}:{self.account}:database/datalake_db"
        )
        glue_tables_arn = (
            f"arn:aws:glue:{self.region}:{self.account}:table/datalake_db/*"
        )

        # --- Crawler Role: mínimo privilegio (Requisito 7.3) ---
        # Rol que asume el crawler. Igual que el rol del job, NO se adjunta la
        # política administrada AWSGlueServiceRole porque concede Resource "*";
        # cada permiso se otorga de forma explícita con ARNs concretos.
        self.crawler_role = iam.Role(
            self,
            "CrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description=(
                "Rol de minimo privilegio del Glue Crawler: lee processed/ y "
                "cataloga tablas en datalake_db, sin wildcards de recurso."
            ),
        )

        # Lectura de los objetos Parquet de la zona processed/ (el crawler los
        # muestrea para inferir el esquema). ARN concreto bajo el prefijo
        # processed/, nunca "*".
        self.crawler_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadProcessedObjects",
                actions=["s3:GetObject"],
                resources=[self.data_lake_bucket.arn_for_objects("processed/*")],
            )
        )

        # Listado del bucket acotado por condición de prefijo a processed/. El
        # Resource es el ARN concreto del bucket (no "*") y la condición
        # `s3:prefix` evita listar raw/ o curated/.
        self.crawler_role.add_to_policy(
            iam.PolicyStatement(
                sid="ListBucketScopedToProcessed",
                actions=["s3:ListBucket"],
                resources=[self.data_lake_bucket.bucket_arn],
                conditions={"StringLike": {"s3:prefix": ["processed/*"]}},
            )
        )

        # Permisos del Data Catalog acotados a `datalake_db` y sus tablas. El
        # crawler crea/actualiza tablas y particiones e inspecciona la base de
        # datos; los ARNs concretos limitan el alcance al catálogo de la cuenta,
        # la base `datalake_db` y sus tablas, nunca a otras bases ni a "*".
        self.crawler_role.add_to_policy(
            iam.PolicyStatement(
                sid="CatalogTablesAndPartitions",
                actions=[
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:BatchGetPartition",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:CreatePartition",
                    "glue:UpdatePartition",
                    "glue:DeletePartition",
                    "glue:BatchCreatePartition",
                    "glue:BatchDeletePartition",
                ],
                resources=[
                    glue_catalog_arn,
                    glue_database_arn,
                    glue_tables_arn,
                ],
            )
        )

        # Logs del crawler en CloudWatch con ARN concreto del grupo de logs de
        # Glue (arn:aws:logs:<region>:<account>:log-group:/aws-glue/*), nunca "*".
        self.crawler_role.add_to_policy(
            iam.PolicyStatement(
                sid="CrawlerCloudWatchLogs",
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=[
                    f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws-glue/*"
                ],
            )
        )

        # --- CfnCrawler: target processed/, salida datalake_db, sin schedule ---
        # Se usa el recurso de bajo nivel `CfnCrawler` para tener control explícito
        # sobre el target S3, la base de salida y la política de cambios de esquema.
        self.crawler = glue.CfnCrawler(
            self,
            "ProcessedCrawler",
            role=self.crawler_role.role_arn,
            # Salida en la base `datalake_db` (Requisito 4.2). Se referencia el
            # nombre físico de forma estable; el crawler crea/actualiza tablas ahí.
            database_name="datalake_db",
            targets=glue.CfnCrawler.TargetsProperty(
                # Target S3 al prefijo processed/ del bucket (Requisito 4.2). El
                # crawler recorre los Parquet particionados por fecha y deriva el
                # esquema y las particiones (Requisito 4.3).
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{self.data_lake_bucket.bucket_name}/processed/"
                    )
                ]
            ),
            # SIN `schedule`: el crawler no se ejecuta por tiempo. Solo se activa
            # por el trigger condicional (job SUCCEEDED) o bajo demanda
            # (Requisito 4.6). Omitir la propiedad equivale a "ON DEMAND".
            # Política de cambios de esquema que preserva las tablas previas ante
            # un fallo del crawler (Requisito 4.7): actualiza el esquema en el
            # catálogo cuando detecta cambios, pero registra (LOG) los objetos
            # borrados en origen en lugar de eliminar las tablas/particiones del
            # catálogo, de modo que un fallo no destruye lo ya catalogado.
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
            description=(
                "Crawler que cataloga los Parquet de processed/ en datalake_db; "
                "sin schedule, se dispara al finalizar el job o bajo demanda."
            ),
        )

        # --- CfnTrigger condicional: job SUCCEEDED -> start crawler ---
        # Encadena el pipeline dentro del dominio de Glue sin una segunda Lambda:
        # cuando el Glue Job termina con estado SUCCEEDED, el trigger inicia el
        # crawler (Requisito 4.4). `start_on_creation=True` deja el trigger activo
        # desde el deploy. El predicado referencia el nombre del job y la acción
        # referencia el nombre del crawler (ambos vía `ref` de CloudFormation,
        # sin literales).
        self.crawler_trigger = glue.CfnTrigger(
            self,
            "JobSuccessCrawlerTrigger",
            type="CONDITIONAL",
            start_on_creation=True,
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=self.transform_job.ref,
                        state="SUCCEEDED",
                    )
                ]
            ),
            actions=[
                glue.CfnTrigger.ActionProperty(crawler_name=self.crawler.ref)
            ],
            description=(
                "Dispara el crawler cuando el Glue Transform Job finaliza con "
                "estado SUCCEEDED."
            ),
        )

        # Nombre del crawler para tareas posteriores (p. ej. documentación o
        # ejecución bajo demanda). Para AWS::Glue::Crawler, `ref` resuelve al
        # nombre del crawler.
        self.crawler_name = self.crawler.ref
