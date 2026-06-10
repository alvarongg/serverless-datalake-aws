# Producto

## Qué es

**serverless-datalake-aws** es un Data Lake serverless completo en AWS, desplegable en una
cuenta propia con un solo `cdk deploy`. Está definido íntegramente con AWS CDK v2 en Python,
dentro de un único stack.

El proyecto es **educativo**: acompaña una serie de blog posts. Por eso el código prioriza la
**claridad y la legibilidad** por sobre la optimización prematura.

## Audiencia

Data engineers de habla hispana que están aprendiendo arquitecturas serverless de datos en AWS.
Se asume familiaridad básica con Python y la consola de AWS, pero no con CDK ni con cada servicio
involucrado.

## Escenario de negocio

Las sucursales de una empresa suben sus ventas diarias como archivos CSV. El pipeline las
transforma automáticamente en datos consultables por SQL:

1. Una sucursal sube `ventas-2025-06-10.csv` a la zona `raw/` del bucket.
2. El evento dispara el pipeline sin intervención manual.
3. Los datos quedan transformados a Parquet particionado por fecha y catalogados.
4. Un analista los consulta con SQL desde Athena.

## Principio rector

**Cada servicio hace una sola cosa, conectados por eventos.** Arquitectura event-driven y
desacoplada: el almacenamiento notifica, una función orquesta, un job transforma, un crawler
cataloga y un motor de consultas expone. Ningún componente conoce los detalles internos del otro;
se comunican por eventos y por convenciones de ubicación en el bucket.

## Objetivos de diseño

- Desplegable de punta a punta con `cdk bootstrap` + `cdk deploy`.
- Destruible limpio con `cdk destroy`, sin recursos huérfanos.
- Sin costos fijos mensuales: todo serverless, pago por uso.
- Seguro por defecto: cifrado, sin acceso público, roles de mínimo privilegio.
