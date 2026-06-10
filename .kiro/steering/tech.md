# Stack técnico

## Lenguaje

- **Python 3.12** para todo: infraestructura (CDK), código de Lambda, script del Glue Job y
  utilidades.

## Infraestructura como código

- **AWS CDK v2** (`aws-cdk-lib`).
- **Un único stack** definido en `datalake/datalake_stack.py` (clase `DataLakeStack`).
- No dividir en stacks anidados ni múltiples stacks.
- Región y cuenta **parametrizables vía environment**, nunca hardcodeadas.

## Servicios AWS

- **Amazon S3**: bucket único con zonas por prefijo (`raw/`, `processed/`, `curated/`).
- **AWS Lambda**: orquestador que dispara el pipeline ante eventos de S3.
- **AWS Glue**: Job (transformación), Crawler (catalogación) y Data Catalog (base de datos
  `datalake_db`).
- **Amazon Athena**: Workgroup propio con ubicación de resultados y límite de bytes escaneados.
- **AWS Lake Formation**: registro del bucket como location y permisos de grano fino.

## Organización del código

- El código de **Lambda vive en `lambda_src/`** — asset separado de la infraestructura.
- El **script del Glue Job vive en `glue_src/`** — asset separado de la infraestructura.
- El stack referencia estos directorios como assets; nunca se mezcla lógica de runtime con la
  definición de recursos.

## Formato de datos

- Datos crudos: **CSV** en `raw/`.
- Datos procesados: **Parquet particionado por fecha** en `processed/`.

## Testing

- **pytest** con assertions de CDK (`aws_cdk.assertions.Template`).
- Los tests validan propiedades de seguridad y configuración del template sintetizado, no
  despliegan recursos reales.

## Dependencias

- Sin frameworks adicionales: **boto3 y la librería estándar alcanzan**.
- `requirements.txt` mínimo: `aws-cdk-lib`, `constructs`, `pytest`, `boto3`.

## Comandos habituales

```bash
# Preparar entorno
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Sintetizar / desplegar / destruir
cdk synth
cdk bootstrap        # solo la primera vez por cuenta/región
cdk deploy
cdk destroy

# Tests
pytest
```
