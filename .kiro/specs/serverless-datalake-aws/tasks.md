# Implementation Plan: serverless-datalake-aws

## Overview

Este plan convierte el diseño de **serverless-datalake-aws** en una serie de tareas de codificación
incrementales. Cada tarea construye sobre las anteriores y termina con un commit + push al remoto
(formato convencional: `feat:`, `fix:`, `test:`, `docs:`), de modo que el historial de Git cuente la
historia completa del proyecto.

Convenciones de Git aplicadas a todo el plan:

- Un commit (y `git push`) al completar cada tarea del spec.
- Si una tarea toca varios archivos relacionados (p. ej. el Glue Job y su rol IAM), todos van en el
  **mismo commit**; nunca se mezclan tareas distintas en un commit.
- La primera tarea incluye en su commit los steering files y el spec (`.kiro/`) además del
  scaffolding, para que el historial arranque con la historia completa.
- El push se hace a una rama de trabajo, nunca directo a `main`/`master`, salvo indicación expresa.

Lenguaje de implementación: **Python 3.12** (AWS CDK v2), según el diseño y los steering files.

## Tasks

- [-] 1. Estructura base del proyecto y parametrización de cuenta/región (`app.py`)
  - Crear el scaffolding de carpetas: `datalake/`, `lambda_src/`, `glue_src/`, `sample_data/`,
    `tests/` (con `__init__.py` donde corresponda para que sean paquetes importables).
  - Crear `requirements.txt` con dependencias mínimas: `aws-cdk-lib`, `constructs`, `pytest`,
    `boto3`, `hypothesis`, `pandas`, `pyarrow`.
  - Crear `.gitignore` (Python, CDK: `cdk.out/`, `.venv/`, `__pycache__/`, `*.pyc`, etc.).
  - Crear `LICENSE` con el texto de la licencia MIT.
  - Crear `cdk.json` apuntando a `python app.py` y con contexto por defecto configurable.
  - Implementar `app.py`: función pura de resolución de environment que obtiene cuenta y región
    desde el environment de CDK (`CDK_DEFAULT_ACCOUNT`/`CDK_DEFAULT_REGION`), con fallback a la
    variable de entorno `DATALAKE_REGION` o al contexto de CDK; sin literales de cuenta/región en
    el código del stack; aborta con error claro indicando cuál de los dos valores falta.
  - Crear un esqueleto mínimo de `datalake/datalake_stack.py` (clase `DataLakeStack` vacía que
    sintetiza sin recursos) para que `app.py` instancie el stack y `cdk synth` no falle.
  - Commit + push: `git add .` (incluye scaffolding, `requirements.txt`, `.gitignore`, `LICENSE`,
    `cdk.json`, `app.py`, los steering files y el spec en `.kiro/`) y
    `git commit -m "feat: estructura base del proyecto y parametrización de cuenta/región"`,
    luego `git push -u origin <rama>`.
  - _Requisitos: 11.1, 11.2, 11.3_

- [ ] 2. Data_Lake_Bucket (S3) seguro con tres zonas
  - [~] 2.1 Implementar el bucket en `DataLakeStack`
    - Crear el `Data_Lake_Bucket` con `encryption=BucketEncryption.S3_MANAGED` (SSE-S3),
      `block_public_access=BlockPublicAccess.BLOCK_ALL` (las cuatro flags en `true`),
      `versioned=True`, `removal_policy=RemovalPolicy.DESTROY`, `auto_delete_objects=True` y
      `enforce_ssl=True`.
    - Documentar en comentarios (español) que las zonas `raw/`, `processed/`, `curated/` y
      `athena-results/` son prefijos lógicos, no recursos.
    - Commit + push: `git commit -m "feat: bucket S3 seguro con tres zonas y removal policy destroy"`.
    - _Requisitos: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8_

- [ ] 3. Glue: catálogo, Job de transformación y Crawler con sus roles IAM
  - [~] 3.1 Crear la base de datos del catálogo (`datalake_db`)
    - Añadir `CfnDatabase` con nombre `datalake_db` en el `Glue_Data_Catalog`.
    - Commit + push: `git commit -m "feat: base de datos datalake_db en el Glue Data Catalog"`.
    - _Requisitos: 4.1_

  - [~] 3.2 Implementar el script del Glue Job (`glue_src/transform_job.py`)
    - Implementar `extraer_fecha(nombre_archivo)`: extrae la fecha `AAAA-MM-DD` del nombre del
      archivo de origen y lanza error si no hay una fecha válida.
    - Implementar `leer_csv`, la transformación a Parquet con columna de partición `fecha`, y la
      escritura atómica vía staging (`processed/_staging/<run-id>/`) que limpia ante fallo y no
      deja particiones parciales.
    - Validación temprana: si la key no termina en `.csv` o el CSV no es parseable, falla sin
      escribir nada en `processed/`.
    - Commit + push: `git commit -m "feat: script del Glue Job CSV a Parquet con escritura atómica"`.
    - _Requisitos: 3.1, 3.2, 3.3, 3.4, 3.5_

  - [~] 3.3 Test de propiedad para la extracción de fecha
    - **Property 2: Round-trip de extracción de fecha del nombre de archivo**
    - **Validates: Requisitos 3.3**

  - [~] 3.4 Test de propiedad para la transformación CSV→Parquet
    - **Property 3: La transformación preserva los datos y particiona por la fecha de origen**
    - **Validates: Requisitos 3.2, 3.3**

  - [~] 3.5 Unit tests del Glue Job (casos de error)
    - CSV inválido / key inexistente / no `.csv`: falla sin escribir en `processed/`.
    - Fallo de escritura a mitad: no deja particiones parciales.
    - _Requisitos: 3.4, 3.5_

  - [~] 3.6 Crear el Glue_Transform_Job (Python Shell) y su rol IAM
    - Definir el `CfnJob` tipo Python Shell que referencia `glue_src/transform_job.py` como asset.
    - Crear el Glue Job Role de mínimo privilegio: `s3:GetObject` sobre `raw/*`, `s3:PutObject` y
      `s3:DeleteObject` sobre `processed/*`, `s3:ListBucket` con condición de prefijo, logs de
      Glue; sin escritura sobre `raw/` ni acceso a `curated/`. Todos los `Resource` con ARN
      concreto, ninguno `"*"`.
    - El job y su rol van en el mismo commit (archivos relacionados).
    - Commit + push: `git commit -m "feat: Glue Transform Job Python Shell con rol IAM de minimo privilegio"`.
    - _Requisitos: 3.1, 7.2, 7.3, 7.5_

  - [~] 3.7 Crear el Glue_Crawler, su rol IAM y el Glue Trigger condicional
    - Definir el `CfnCrawler` con target `s3://<bucket>/processed/`, salida `datalake_db`, sin
      `schedule`, y `schema_change_policy` que preserve tablas previas ante fallo.
    - Crear el Crawler Role de mínimo privilegio: `s3:GetObject`/`s3:ListBucket` sobre `processed/`
      y permisos de catálogo (`glue:*Table`, `glue:*Partition`, `glue:GetDatabase`) sobre
      `datalake_db` y sus tablas, con ARNs concretos.
    - Definir el `CfnTrigger` tipo `CONDITIONAL`: predicado job `SUCCEEDED` → acción start crawler.
    - Crawler, rol y trigger van en el mismo commit (archivos relacionados).
    - Commit + push: `git commit -m "feat: Glue Crawler con rol IAM y trigger condicional al job"`.
    - _Requisitos: 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 7.3_

- [~] 4. Checkpoint - Asegurar que la síntesis y los tests pasan
  - Ejecutar `cdk synth` y `pytest`. Asegurar que todos los tests pasan; consultar al usuario si
    surgen dudas.

- [ ] 5. Trigger_Lambda y notificación de eventos S3
  - [~] 5.1 Implementar el handler de la Lambda (`lambda_src/trigger_pipeline.py`)
    - Implementar `handler(event, context)` que, por cada record, extrae bucket/key, aplica la
      decisión de filtrado (prefijo `raw/` y sufijo `.csv`), inicia exactamente una ejecución del
      Glue Job con `start_job_run` pasando `--bucket` y `--key`, y ante fallo registra un log de
      error con bucket y key y relanza la excepción sin tocar el objeto en `raw/`.
    - Aislar la decisión de filtrado en una función pura (p. ej. `debe_procesar(key)`) para poder
      testearla con property-based testing.
    - Commit + push: `git commit -m "feat: handler Lambda que orquesta el Glue Job con filtrado raw/csv"`.
    - _Requisitos: 2.2, 2.3, 2.4, 2.5, 2.6_

  - [~] 5.2 Test de propiedad para la decisión de filtrado de la Lambda
    - **Property 1: Decisión de filtrado de la Lambda**
    - **Validates: Requisitos 2.3, 2.6**

  - [~] 5.3 Unit tests de la Lambda con mocks
    - Inicia exactamente una ejecución con `JobName` y `Arguments` correctos.
    - Ante fallo de `start_job_run`: relanza error y no borra el objeto.
    - _Requisitos: 2.4, 2.5_

  - [~] 5.4 Crear la Trigger_Lambda, su rol IAM y la notificación S3
    - Definir la `Function` (`PYTHON_3_12`, `timeout=60s`, código desde `lambda_src/`, variable de
      entorno `GLUE_JOB_NAME`).
    - Crear el Lambda Execution Role de mínimo privilegio: `glue:StartJobRun` restringido al ARN
      exacto del Glue_Transform_Job (único recurso), más logs de CloudWatch con ARN concreto.
    - Configurar la notificación del bucket con `add_event_notification(OBJECT_CREATED, ...,
      NotificationKeyFilter(prefix="raw/", suffix=".csv"))`.
    - Lambda, rol y notificación van en el mismo commit (archivos relacionados).
    - Commit + push: `git commit -m "feat: Trigger Lambda con rol StartJobRun por ARN y notificacion S3 raw/csv"`.
    - _Requisitos: 2.1, 2.2, 2.3, 7.1, 7.3, 7.4_

- [ ] 6. Athena_Workgroup con protección de costos
  - [~] 6.1 Implementar el workgroup de Athena
    - Crear el `CfnWorkGroup` dedicado (no `primary`) con
      `result_configuration.output_location = s3://<bucket>/athena-results/`,
      `bytes_scanned_cutoff_per_query = 1_073_741_824` (1 GiB),
      `enforce_work_group_configuration = True` y `recursive_delete_option = True`.
    - Commit + push: `git commit -m "feat: Athena Workgroup con limite de 1 GiB y configuracion forzada"`.
    - _Requisitos: 5.1, 5.2, 5.3, 5.4, 5.5_

- [ ] 7. Lake_Formation: location, Analyst_Role y permiso SELECT
  - [~] 7.1 Registrar la location, crear el Analyst_Role y otorgar SELECT
    - Registrar el bucket como location de Lake Formation (`use_service_linked_role=True`) con
      dependencia explícita respecto del bucket (`node.add_dependency(bucket)`).
    - Crear el `Analyst_Role` asumible por un `AccountPrincipal`, sin políticas IAM directas de
      lectura sobre S3.
    - Otorgar `SELECT` sobre `datalake_db` y sus tablas vía `CfnPermissions`, sin
      `INSERT`/`ALTER`/`DROP`/`DELETE`.
    - Incluir el patrón comentado (sin efecto sobre el template) para extender permisos a otros
      principales/recursos.
    - Location, rol y permisos van en el mismo commit (archivos relacionados).
    - Commit + push: `git commit -m "feat: Lake Formation location, Analyst_Role y permiso SELECT de grano fino"`.
    - _Requisitos: 6.1, 6.2, 6.3, 6.4, 6.5_

- [ ] 8. Outputs del stack
  - [~] 8.1 Implementar los cuatro outputs
    - Añadir exactamente cuatro `CfnOutput`: `DataLakeBucketName`, `GlueTransformJobName`,
      `AthenaWorkgroupName`, `DatalakeDbName`, cada uno con descripción en español, no vacía y
      única. No agregar ningún output adicional.
    - Commit + push: `git commit -m "feat: cuatro outputs del stack con descripciones en espanol"`.
    - _Requisitos: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7_

- [~] 9. Checkpoint - Asegurar que la síntesis y los tests pasan
  - Ejecutar `cdk synth` y `pytest`. Asegurar que todos los tests pasan; consultar al usuario si
    surgen dudas.

- [ ] 10. Generador de datos de ejemplo (`sample_data/generate_ventas.py`)
  - [~] 10.1 Implementar el Data_Generator standalone
    - Script ejecutable de forma independiente (solo librería estándar: `csv`, `random`,
      `datetime`, `argparse`), sin credenciales ni recursos AWS.
    - Columnas exactas y en orden: `fecha`, `sucursal`, `producto`, `cantidad`, `monto` con fila
      de encabezado; entre 100 y 1000 filas; `fecha` en `AAAA-MM-DD`, `cantidad` entero 1–1000,
      `monto` decimal 0.01–999999.99 con dos decimales; UTF-8 y separador coma.
    - Manejo de error de escritura: escribe a archivo temporal y renombra al final; ante fallo,
      sale con código ≠ 0, emite la causa y no deja archivo parcial.
    - Commit + push: `git commit -m "feat: generador standalone de CSV de ventas ficticias"`.
    - _Requisitos: 9.1, 9.2, 9.3, 9.4, 9.5_

  - [~] 10.2 Test de propiedad para el generador de datos
    - **Property 4: El CSV generado siempre cumple el esquema y los rangos**
    - **Validates: Requisitos 9.2, 9.3, 9.4**

  - [~] 10.3 Unit test del generador ante ruta no escribible
    - Exit ≠ 0, mensaje de causa y sin archivo parcial.
    - _Requisitos: 9.5_

- [ ] 11. Suite de tests del template y propiedades de corrección (`tests/test_datalake_stack.py`)
  - [~] 11.1 Implementar las CDK assertions del template
    - Sintetizar el stack en memoria con `aws_cdk.assertions.Template` (sin desplegar) y verificar:
      cifrado en reposo del bucket; las cuatro flags de bloqueo de acceso público en `true`;
      exactamente un filtro de prefijo con valor `raw/` en la notificación; configuración estática
      restante (runtime/timeout de la Lambda, ARN único en `glue:StartJobRun`, recursos del Glue
      Job Role sin `curated/`, workgroup con enforce + 1 GiB + `athena-results/`, crawler sin
      schedule, Lake Formation location con DependsOn, Analyst_Role sin IAM directo a S3).
    - Commit + push: `git commit -m "test: CDK assertions de seguridad y configuracion del template"`.
    - _Requisitos: 10.1, 10.2, 10.3, 10.4, 10.6_

  - [~] 11.2 Test de propiedad: ningún rol IAM concede Resource "*"
    - **Property 6: Ningún rol IAM concede `Resource: "*"`**
    - **Validates: Requisitos 7.3, 10.5**

  - [~] 11.3 Test de propiedad: outputs con descripción no vacía y única
    - **Property 7: Los outputs tienen descripción no vacía y única**
    - **Validates: Requisitos 8.6**

  - [~] 11.4 Test de propiedad para la resolución de cuenta/región
    - **Property 5: Resolución de cuenta/región según precedencia y sin valores hardcodeados**
    - **Validates: Requisitos 11.1, 11.2, 11.3**

  - [~] 11.5 Unit test de la resolución de environment sin cuenta ni región
    - Error que indica cuál de los dos valores falta, sin crear recursos.
    - _Requisitos: 11.3_

- [~] 12. Checkpoint - Asegurar que toda la suite pasa
  - Ejecutar `cdk synth` y `pytest`. Asegurar que todos los tests pasan; consultar al usuario si
    surgen dudas.

- [ ] 13. README en español (`README.md`)
  - [~] 13.1 Escribir el README del proyecto
    - Incluir: diagrama de arquitectura (event-driven, los cinco saltos), prerequisitos, deploy
      (`cdk bootstrap` + `cdk deploy`), cómo probar el pipeline de punta a punta (generar CSV,
      subirlo a `raw/ventas/`, consultar en Athena), costos estimados y cómo destruir el stack
      (`cdk destroy`).
    - Commit + push: `git commit -m "docs: README en espanol con arquitectura, deploy, pruebas y costos"`.
    - _Requisitos: (documentación de soporte; cubre el flujo de 1.1–11.3 a nivel de uso)_

## Notes

- Las tareas marcadas con `*` son opcionales (tests) y pueden saltarse para un MVP más rápido; las
  tareas de implementación principal nunca se marcan como opcionales.
- Cada tarea referencia los sub-requisitos específicos que cubre, para trazabilidad.
- Los checkpoints aseguran validación incremental ejecutando `cdk synth` y `pytest`.
- Los tests de propiedad (Hypothesis, mínimo 100 iteraciones, una propiedad por test) validan la
  lógica pura: filtrado de la Lambda, extracción de fecha, transformación, generador y resolución
  de region. Las Properties 6 y 7 se implementan como aserciones universales dentro de la
  CDK_Test_Suite.
- Los unit tests con mocks (`unittest.mock`/`moto`) cubren ejemplos y casos de error.
- Flujo de Git: cada tarea termina con un commit (convencional) y push; archivos relacionados de una
  misma tarea van en un solo commit; la tarea 1 incluye además los steering files y el spec.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1"] },
    { "id": 1, "tasks": ["2.1", "3.1", "3.2", "5.1", "10.1"] },
    { "id": 2, "tasks": ["3.3", "3.4", "3.5", "3.6", "5.2", "5.3", "10.2", "10.3"] },
    { "id": 3, "tasks": ["3.7", "5.4"] },
    { "id": 4, "tasks": ["6.1", "7.1"] },
    { "id": 5, "tasks": ["8.1"] },
    { "id": 6, "tasks": ["11.1", "11.2", "11.3", "11.4", "11.5"] },
    { "id": 7, "tasks": ["13.1"] }
  ]
}
```
