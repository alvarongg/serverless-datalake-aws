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

from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_athena as athena
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lakeformation as lakeformation
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_assets as s3_assets
from aws_cdk import aws_s3_notifications as s3n
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

        # === Trigger_Lambda, su rol IAM y la notificación S3 ===
        # La Lambda orquesta el pipeline: ante la creación de un CSV en raw/,
        # inicia exactamente una ejecución del Glue Transform Job. El código de
        # runtime vive en lambda_src/ como ASSET separado de la infraestructura
        # (steering): nunca se mezcla la lógica de la función con la definición
        # de recursos.
        #
        # La Lambda, su rol y la notificación del bucket se definen juntos porque
        # son recursos estrechamente relacionados: el rol concede el permiso de
        # StartJobRun sobre el job, la función asume ese rol y la notificación
        # conecta el evento de S3 con la función.

        # --- ARN concreto del Glue Transform Job (mínimo privilegio) ---
        # Se construye el ARN exacto del job a partir de región y cuenta resueltas
        # por `app.py` y del nombre del job (`self.transform_job.ref`, un token de
        # CloudFormation que resuelve al nombre físico). Formato:
        #   arn:aws:glue:<region>:<account>:job/<job-name>
        # Este ARN es el ÚNICO recurso de la sentencia StartJobRun (Requisito 7.1,
        # 7.4): la Lambda solo puede iniciar este job, ningún otro, y nunca "*".
        transform_job_arn = (
            f"arn:aws:glue:{self.region}:{self.account}:job/{self.transform_job.ref}"
        )

        # --- Lambda Execution Role: mínimo privilegio (Requisito 7.1, 7.3, 7.4) ---
        # Se define el rol de forma EXPLÍCITA en lugar de dejar que CDK adjunte la
        # política administrada AWSLambdaBasicExecutionRole, porque esa política
        # concede permisos de logs con Resource "*". Aquí cada permiso se otorga
        # con un ARN concreto, garantizando que ningún Resource sea "*"
        # (Property 6 / Requisito 7.3).
        self.trigger_lambda_role = iam.Role(
            self,
            "TriggerLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description=(
                "Rol de minimo privilegio de la Trigger Lambda: StartJobRun "
                "solo sobre el Glue Transform Job y logs de CloudWatch, sin "
                "wildcards de recurso."
            ),
        )

        # Permiso para iniciar ÚNICAMENTE el Glue Transform Job, identificado por
        # su ARN exacto como único recurso de la sentencia (Requisito 7.1, 7.4).
        self.trigger_lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="StartTransformJobRun",
                actions=["glue:StartJobRun"],
                resources=[transform_job_arn],
            )
        )

        # Logs de CloudWatch con ARN concreto, nunca "*". Se usa el patrón de
        # grupos de logs de Lambda (/aws/lambda/*) en lugar del nombre exacto de
        # la función para evitar una dependencia circular (el nombre del grupo
        # depende del nombre de la función, que a su vez necesita este rol). El
        # ARN sigue siendo concreto: acota a los grupos de logs de Lambda de esta
        # cuenta y región, sin abrir el permiso a cualquier recurso.
        self.trigger_lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="LambdaCloudWatchLogs",
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=[
                    f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/lambda/*:*"
                ],
            )
        )

        # --- Trigger_Lambda (Function) ---
        # Runtime Python 3.12 y timeout de 60 s (Requisito 2.1). El código se toma
        # del directorio lambda_src/ como asset; el handler es
        # `trigger_pipeline.handler`. La variable de entorno GLUE_JOB_NAME lleva
        # el nombre del job para que el handler lo use en `start_job_run`.
        self.trigger_lambda = lambda_.Function(
            self,
            "TriggerLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(60),
            handler="trigger_pipeline.handler",
            code=lambda_.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "..", "lambda_src")
            ),
            role=self.trigger_lambda_role,
            environment={
                # Nombre del Glue Job que la Lambda debe iniciar (Requisito 2.4).
                "GLUE_JOB_NAME": self.transform_job.ref,
            },
            description=(
                "Trigger Lambda que inicia el Glue Transform Job ante la "
                "creacion de un CSV en raw/."
            ),
        )

        # --- Notificación S3 -> Lambda (filtrada por raw/ y .csv) ---
        # El bucket invoca la Lambda solo ante OBJECT_CREATED de objetos cuyo
        # prefijo es raw/ y cuyo sufijo es .csv (Requisito 2.2, 2.3, 2.6). Este
        # filtro es crítico para evitar loops infinitos: las escrituras del Glue
        # Job en processed/ no coinciden con el filtro y por tanto no re-disparan
        # la Lambda.
        self.data_lake_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.trigger_lambda),
            s3.NotificationKeyFilter(prefix="raw/", suffix=".csv"),
        )

        # === Athena_Workgroup con protección de costos ===
        # Workgroup PROPIO y dedicado a las consultas del data lake, separado del
        # workgroup `primary` por defecto (Requisito 5.1). Un workgroup dedicado
        # permite aplicar protecciones de costo y una ubicación de resultados
        # fijas a todas las consultas del proyecto, sin tocar el workgroup global.
        #
        # Se fija un nombre físico explícito (`datalake_workgroup`) para que el
        # output del stack pueda resolver un nombre estable y legible, y para que
        # los analistas seleccionen el workgroup por nombre en la consola/CLI de
        # Athena.
        self.athena_workgroup_name = "datalake_workgroup"
        self.athena_workgroup = athena.CfnWorkGroup(
            self,
            "DatalakeWorkgroup",
            name=self.athena_workgroup_name,
            description=(
                "Workgroup dedicado del data lake con limite de bytes escaneados "
                "y ubicacion de resultados forzada."
            ),
            # `recursive_delete_option=True` permite que `cdk destroy` elimine el
            # workgroup aunque conserve consultas guardadas o historial, evitando
            # recursos huérfanos en el proyecto educativo.
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                # `enforce_work_group_configuration=True`: la configuración del
                # workgroup (ubicación de resultados y límite de bytes) prevalece
                # sobre cualquier ajuste enviado por el cliente, de modo que
                # ninguna consulta pueda anular la protección de costos
                # (Requisito 5.2).
                enforce_work_group_configuration=True,
                # Límite máximo de bytes escaneados por consulta: 1 GiB
                # (1.073.741.824 bytes). Si una consulta intenta escanear más,
                # Athena la cancela sin resultados parciales y devuelve un error
                # de límite superado (Requisito 5.4, 5.5).
                bytes_scanned_cutoff_per_query=1_073_741_824,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    # Los resultados de las consultas se almacenan en el propio
                    # Data_Lake_Bucket bajo el prefijo `athena-results/`
                    # (Requisito 5.3).
                    output_location=(
                        f"s3://{self.data_lake_bucket.bucket_name}/athena-results/"
                    ),
                ),
            ),
        )

        # === Lake_Formation: location, Analyst_Role y permiso SELECT ===
        # Lake Formation centraliza el control de acceso de grano fino sobre el
        # catálogo de datos. En lugar de conceder permisos IAM directos sobre S3
        # a cada consumidor, se registra el bucket como "location" y se otorgan
        # permisos a nivel de base de datos/tabla (p. ej. SELECT) a los
        # principales que correspondan. Esto demuestra el patrón de seguridad de
        # un data lake gobernado por catálogo.
        #
        # La location, el rol de ejemplo y su permiso SELECT se definen juntos
        # porque son recursos estrechamente relacionados: el permiso concede al
        # rol acceso de lectura sobre las tablas catalogadas en `datalake_db`.

        # --- Registro del bucket como location de Lake Formation ---
        # `use_service_linked_role=True` registra el bucket delegando la
        # administración de permisos en el rol vinculado al servicio de Lake
        # Formation (modo recomendado): a partir de aquí, el acceso a los datos
        # de este bucket se gobierna por permisos de Lake Formation y no solo por
        # políticas IAM (Requisito 6.1).
        self.lf_location = lakeformation.CfnResource(
            self,
            "DataLakeLocation",
            resource_arn=self.data_lake_bucket.bucket_arn,
            use_service_linked_role=True,
        )

        # Dependencia explícita respecto del bucket: garantiza que el bucket
        # exista antes de intentar registrarlo como location (Requisito 6.5).
        # CDK no infiere esta dependencia automáticamente porque solo se referencia
        # el ARN (un atributo), por eso se declara a mano.
        self.lf_location.node.add_dependency(self.data_lake_bucket)

        # --- Analyst_Role: rol de ejemplo de un analista de datos ---
        # Rol asumible por cualquier principal de la MISMA cuenta
        # (`AccountPrincipal`). Es deliberadamente un rol SIN políticas IAM
        # directas de lectura sobre S3: el acceso a los datos NO se concede por
        # IAM sino exclusivamente vía Lake Formation (Requisito 6.2). Así se
        # ilustra el control de acceso de grano fino gobernado por el catálogo.
        self.analyst_role = iam.Role(
            self,
            "AnalystRole",
            assumed_by=iam.AccountPrincipal(self.account),
            description=(
                "Rol de ejemplo de analista de datos. NO tiene politicas IAM de "
                "lectura sobre S3: el acceso a los datos se controla unicamente "
                "via Lake Formation (permiso SELECT sobre datalake_db)."
            ),
        )

        # --- Permiso SELECT de grano fino sobre datalake_db y sus tablas ---
        # Se concede al Analyst_Role el permiso SELECT sobre TODAS las tablas de
        # `datalake_db` mediante `table_wildcard` (Requisito 6.3). Solo SELECT:
        # NO se otorga INSERT, ALTER, DROP ni DELETE, de modo que el analista
        # puede consultar pero no modificar el catálogo ni los datos.
        self.analyst_select_permission = lakeformation.CfnPermissions(
            self,
            "AnalystSelectPermission",
            permissions=["SELECT"],
            data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=self.analyst_role.role_arn,
            ),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                # `table_wildcard={}` aplica el permiso a todas las tablas de la
                # base, incluidas las que el crawler cree en el futuro.
                table_resource=lakeformation.CfnPermissions.TableResourceProperty(
                    catalog_id=self.account,
                    database_name="datalake_db",
                    table_wildcard={},
                ),
            ),
        )

        # Dependencia explícita respecto de la base de datos: el permiso debe
        # crearse DESPUÉS de que exista `datalake_db` en el catálogo, de lo
        # contrario Lake Formation no encontraría el recurso al que aplicar el
        # SELECT.
        self.analyst_select_permission.node.add_dependency(self.datalake_database)

        # --- Patrón para extender permisos a otros principales/recursos ---
        # El siguiente bloque está COMENTADO a propósito (Requisito 6.4): no tiene
        # efecto sobre el template sintetizado y sirve solo como documentación de
        # cómo otorgar permisos de Lake Formation a otro rol/usuario o sobre otro
        # recurso (otra base, una tabla concreta o columnas específicas). Para
        # usarlo, descomentar y ajustar el principal y el recurso:
        #
        # otro_rol = iam.Role(
        #     self,
        #     "OtroConsumidorRole",
        #     assumed_by=iam.AccountPrincipal(self.account),
        # )
        # lakeformation.CfnPermissions(
        #     self,
        #     "OtroConsumidorSelectPermission",
        #     permissions=["SELECT"],
        #     data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
        #         data_lake_principal_identifier=otro_rol.role_arn,
        #     ),
        #     resource=lakeformation.CfnPermissions.ResourceProperty(
        #         # Ejemplo: permiso sobre UNA tabla concreta en lugar de todas.
        #         table_resource=lakeformation.CfnPermissions.TableResourceProperty(
        #             catalog_id=self.account,
        #             database_name="datalake_db",
        #             name="ventas",
        #         ),
        #     ),
        # )
