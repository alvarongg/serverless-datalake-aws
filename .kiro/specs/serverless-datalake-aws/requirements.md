# Requirements Document

## Introduction

**serverless-datalake-aws** es un Data Lake serverless completo en AWS, definido íntegramente con
AWS CDK v2 en Python dentro de un único stack llamado `DataLakeStack`. El proyecto es educativo:
acompaña una serie de blog posts dirigidos a data engineers de habla hispana que están aprendiendo
arquitecturas serverless de datos en AWS.

El escenario de negocio es el siguiente: las sucursales de una empresa suben sus ventas diarias
como archivos CSV a una zona de aterrizaje (`raw/`); a partir de ahí, un pipeline event-driven y
desacoplado transforma automáticamente esos archivos en datos consultables por SQL. Cada servicio
hace una sola cosa y se comunica con los demás mediante eventos y convenciones de ubicación en el
bucket: el almacenamiento notifica, una función orquesta, un job transforma, un crawler cataloga y
un motor de consultas expone.

Este documento captura los requisitos funcionales y de restricción del sistema. El alcance abarca
la infraestructura (S3, Lambda, Glue, Athena, Lake Formation, IAM), las utilidades de soporte
(generador de datos de ejemplo) y la validación automatizada del template sintetizado mediante
CDK assertions.

## Glossary

- **DataLakeStack**: Único stack de AWS CDK v2 (Python) que define toda la infraestructura del
  proyecto. No se permiten stacks anidados ni múltiples stacks.
- **Data_Lake_Bucket**: Bucket único de Amazon S3 que contiene las tres zonas del data lake,
  organizadas por prefijo: `raw/`, `processed/` y `curated/`.
- **Zona Raw**: Prefijo `raw/` del Data_Lake_Bucket donde las sucursales depositan los CSV crudos.
- **Zona Processed**: Prefijo `processed/` del Data_Lake_Bucket donde se escriben los datos
  transformados a Parquet particionado por fecha.
- **Zona Curated**: Prefijo `curated/` del Data_Lake_Bucket reservado para datos refinados de
  consumo final.
- **Trigger_Lambda**: Función AWS Lambda (Python 3.12) que orquesta el pipeline iniciando el
  Glue_Transform_Job cuando se crea un objeto en la Zona Raw.
- **Glue_Transform_Job**: AWS Glue Job que lee un CSV de la Zona Raw, lo transforma a Parquet
  particionado por fecha y lo escribe en la Zona Processed.
- **Glue_Crawler**: AWS Glue Crawler que cataloga los datos de la Zona Processed en el
  Glue_Data_Catalog.
- **Glue_Data_Catalog**: Catálogo de datos de AWS Glue que contiene la base de datos
  `datalake_db`.
- **Athena_Workgroup**: Workgroup propio de Amazon Athena con ubicación de resultados y límite de
  bytes escaneados por consulta.
- **Lake_Formation**: Servicio AWS Lake Formation usado para registrar el bucket como location y
  otorgar permisos de grano fino sobre el catálogo.
- **Analyst_Role**: Rol IAM de ejemplo que representa a un analista de datos, usado para demostrar
  el otorgamiento de permisos `SELECT` con Lake Formation.
- **Data_Generator**: Script Python standalone (`sample_data/generate_ventas.py`) que genera un
  CSV de ventas ficticias listo para subir a la Zona Raw.
- **CDK_Test_Suite**: Conjunto de pruebas con `aws_cdk.assertions.Template` que valida propiedades
  de seguridad y configuración del template sintetizado sin desplegar recursos reales.
- **OBJECT_CREATED**: Tipo de evento de notificación de Amazon S3 que se emite al crear un objeto.

## Requirements

### Requisito 1: Bucket S3 con tres zonas y configuración segura

**Historia de usuario:** Como data engineer, quiero un bucket S3 único organizado en zonas por
prefijo y seguro por defecto, para almacenar los datos del lake de forma ordenada y protegida.

#### Criterios de aceptación

1. THE DataLakeStack SHALL crear exactamente un (1) Data_Lake_Bucket que soporte los prefijos
   `raw/`, `processed/` y `curated/` como zonas de datos.
2. THE Data_Lake_Bucket SHALL aplicar cifrado en reposo del tipo SSE-S3 a todos los objetos
   almacenados, sin excepción.
3. WHEN un objeto se carga sin cabecera de cifrado especificada, THE Data_Lake_Bucket SHALL
   cifrarlo en reposo con SSE-S3 de forma automática.
4. THE Data_Lake_Bucket SHALL bloquear todo acceso público activando las cuatro opciones de bloqueo
   (BlockPublicAcls, IgnorePublicAcls, BlockPublicPolicy y RestrictPublicBuckets) en valor
   verdadero.
5. IF una política o ACL intenta otorgar acceso público al Data_Lake_Bucket, THEN THE
   Data_Lake_Bucket SHALL rechazar la operación y mantener el bucket sin acceso público.
6. THE Data_Lake_Bucket SHALL mantener el versionado de objetos en estado activado (Enabled).
7. THE Data_Lake_Bucket SHALL configurarse con `RemovalPolicy.DESTROY` y `auto_delete_objects`
   activado.
8. WHEN se ejecuta `cdk destroy`, THE DataLakeStack SHALL eliminar el Data_Lake_Bucket junto con
   todos sus objetos y versiones, sin dejar recursos huérfanos.

### Requisito 2: Lambda disparadora del pipeline filtrada por la zona raw

**Historia de usuario:** Como data engineer, quiero que la subida de un CSV a la zona raw dispare
automáticamente la transformación, para que el pipeline funcione sin intervención manual y sin
crear loops infinitos.

#### Criterios de aceptación

1. THE DataLakeStack SHALL crear una Trigger_Lambda que use el runtime Python 3.12 con un timeout
   de ejecución de 60 segundos.
2. WHEN se produce un evento OBJECT_CREATED sobre un objeto cuyo prefijo es `raw/` y cuyo sufijo es
   `.csv`, THE Trigger_Lambda SHALL ser invocada en un plazo máximo de 5 segundos.
3. THE notificación de eventos del Data_Lake_Bucket SHALL filtrar las invocaciones de la
   Trigger_Lambda únicamente a objetos cuyo prefijo es `raw/`, de modo que los objetos escritos
   bajo los prefijos `processed/` y `curated/` no disparen la Trigger_Lambda.
4. WHEN la Trigger_Lambda es invocada, THE Trigger_Lambda SHALL iniciar exactamente una ejecución
   del Glue_Transform_Job pasando el nombre del bucket y la key del objeto como argumentos.
5. IF la Trigger_Lambda no logra iniciar el Glue_Transform_Job, THEN THE Trigger_Lambda SHALL
   preservar el objeto en la Zona Raw, devolver un estado de error al invocador y registrar un
   mensaje de error que incluya el bucket y la key del objeto.
6. IF un objeto creado bajo el prefijo `raw/` no tiene sufijo `.csv`, THEN THE Trigger_Lambda SHALL
   NOT iniciar el Glue_Transform_Job para ese objeto.

### Requisito 3: Glue Job de transformación CSV a Parquet

**Historia de usuario:** Como data engineer, quiero un job que transforme los CSV crudos a Parquet
particionado por fecha, para que los datos queden en un formato eficiente y consultable por SQL.

#### Criterios de aceptación

1. THE DataLakeStack SHALL crear un Glue_Transform_Job cuyo script reside en `glue_src/` como
   asset separado de la definición de infraestructura.
2. WHEN el Glue_Transform_Job se ejecuta recibiendo como argumentos un bucket existente y una key
   que apunta a un objeto con extensión `.csv` dentro de la Zona Raw, THE Glue_Transform_Job SHALL
   leer el contenido completo del CSV indicado.
3. WHEN la lectura del CSV finaliza correctamente, THE Glue_Transform_Job SHALL escribir los datos
   en formato Parquet dentro de la Zona Processed, particionados por la fecha contenida en el
   nombre del archivo de origen en formato AAAA-MM-DD, preservando todas las filas y columnas del
   CSV de origen.
4. IF la key recibida no existe en el bucket indicado, no apunta a un objeto con extensión `.csv`,
   o el contenido no puede leerse como CSV válido, THEN THE Glue_Transform_Job SHALL finalizar con
   estado de fallo, indicar la causa del error y no escribir ningún objeto en la Zona Processed.
5. IF la escritura del Parquet en la Zona Processed falla una vez iniciada, THEN THE
   Glue_Transform_Job SHALL finalizar con estado de fallo, indicar la causa del error y no dejar
   particiones parcialmente escritas en la Zona Processed.

### Requisito 4: Glue Data Catalog y Crawler bajo demanda

**Historia de usuario:** Como analista de datos, quiero que los datos procesados se cataloguen en
una base de datos consultable, para poder ejecutar consultas SQL sobre ellos sin definir esquemas
manualmente.

#### Criterios de aceptación

1. THE DataLakeStack SHALL crear una base de datos llamada `datalake_db` en el Glue_Data_Catalog.
2. THE DataLakeStack SHALL crear un Glue_Crawler cuyo target apunte al prefijo `processed/` de la
   Zona Processed del bucket y cuya base de datos de salida sea `datalake_db`.
3. WHEN el Glue_Crawler finaliza una ejecución exitosa, THE Glue_Crawler SHALL registrar o
   actualizar en `datalake_db` una tabla por cada conjunto de datos detectado en la Zona Processed,
   con el esquema inferido y las particiones por fecha.
4. WHEN el Glue_Transform_Job finaliza con estado de éxito, THE DataLakeStack SHALL disparar una
   ejecución del Glue_Crawler sin intervención manual.
5. THE Glue_Crawler SHALL exponer un mecanismo de ejecución bajo demanda que permita iniciar una
   ejecución a pedido sin esperar a la finalización del Glue_Transform_Job.
6. THE Glue_Crawler SHALL configurarse sin schedule recurrente, de modo que no se ejecute de forma
   automática por tiempo y solo se active por finalización del Glue_Transform_Job o bajo demanda.
7. IF una ejecución del Glue_Crawler finaliza con error, THEN THE Glue_Crawler SHALL preservar las
   tablas previamente registradas en `datalake_db` sin modificarlas y dejar registrada la causa del
   fallo de la ejecución.

### Requisito 5: Athena Workgroup con protección de costos

**Historia de usuario:** Como analista de datos, quiero un workgroup de Athena propio con límites
de costo, para consultar los datos por SQL sin riesgo de gastos inesperados.

#### Criterios de aceptación

1. THE DataLakeStack SHALL crear un Athena_Workgroup propio y dedicado a las consultas del data
   lake, separado del workgroup `primary` por defecto.
2. THE Athena_Workgroup SHALL aplicar su configuración (ubicación de resultados y límite de bytes
   escaneados) sobre cualquier ajuste del cliente, de modo que ninguna consulta pueda anular la
   protección de costos.
3. THE Athena_Workgroup SHALL almacenar los resultados de las consultas en el Data_Lake_Bucket bajo
   el prefijo `athena-results/`.
4. THE Athena_Workgroup SHALL definir un límite máximo de bytes escaneados por consulta de
   1.073.741.824 bytes (1 GiB) como protección de costos.
5. IF una consulta intenta escanear más bytes que el límite configurado, THEN THE Athena_Workgroup
   SHALL cancelar la consulta, no producir resultados parciales y devolver al analista un error
   indicando que se superó el límite de bytes escaneados.

### Requisito 6: Permisos de grano fino con Lake Formation

**Historia de usuario:** Como data engineer, quiero registrar el bucket en Lake Formation y otorgar
permisos de ejemplo a un rol de analista, para demostrar el patrón de control de acceso de grano
fino.

#### Criterios de aceptación

1. THE DataLakeStack SHALL registrar el Data_Lake_Bucket como location en Lake_Formation con el
   modo de registro que delega la administración de permisos a Lake_Formation.
2. WHEN se sintetiza el template del DataLakeStack, THE DataLakeStack SHALL crear exactamente un
   Analyst_Role de ejemplo, asumible por un principal de la misma cuenta, sin permisos IAM directos
   de lectura sobre el Data_Lake_Bucket (el acceso a datos se controla únicamente vía
   Lake_Formation).
3. THE DataLakeStack SHALL otorgar al Analyst_Role, mediante Lake_Formation, el permiso `SELECT`
   sobre la base de datos `datalake_db` y sus tablas, sin otorgar permisos de escritura,
   modificación ni borrado (`INSERT`, `ALTER`, `DROP`, `DELETE`).
4. THE DataLakeStack SHALL incluir, en forma comentada y sin efecto sobre el template sintetizado,
   el patrón documentado para extender los permisos de Lake_Formation a otros principales o
   recursos.
5. IF el Data_Lake_Bucket aún no existe o no está disponible en el momento de registrar la
   location, THEN THE DataLakeStack SHALL declarar la dependencia explícita de la location respecto
   del bucket, de modo que el registro en Lake_Formation se cree después del bucket.

### Requisito 7: Roles IAM de mínimo privilegio

**Historia de usuario:** Como data engineer responsable de seguridad, quiero que cada servicio
tenga únicamente los permisos que necesita, para minimizar la superficie de ataque y enseñar el
principio de mínimo privilegio.

#### Criterios de aceptación

1. THE DataLakeStack SHALL otorgar a la Trigger_Lambda permiso para iniciar la ejecución únicamente
   del Glue_Transform_Job específico, identificado por su ARN exacto, como único recurso de la
   sentencia IAM que concede ese permiso.
2. THE DataLakeStack SHALL otorgar al Glue_Transform_Job permiso de lectura restringido a los
   objetos de la Zona Raw y permiso de escritura restringido a los objetos de la Zona Processed.
3. THE roles IAM creados por el DataLakeStack SHALL definir, en cada sentencia de sus políticas, el
   elemento Resource con al menos un ARN concreto, sin que ningún Resource sea igual a `"*"`.
4. THE DataLakeStack SHALL NOT otorgar a la Trigger_Lambda permiso para iniciar la ejecución de
   ningún Glue Job distinto del Glue_Transform_Job.
5. THE DataLakeStack SHALL NOT otorgar al Glue_Transform_Job permiso de escritura sobre la Zona Raw
   ni permiso de lectura o escritura sobre la Zona Curated.

### Requisito 8: Outputs del stack

**Historia de usuario:** Como usuario que despliega el stack, quiero ver los identificadores clave
al finalizar el deploy, para poder usar el data lake sin buscarlos manualmente en la consola.

#### Criterios de aceptación

1. WHEN el DataLakeStack se sintetiza, THE DataLakeStack SHALL exponer exactamente un output cuyo
   valor resuelve al nombre real (physical name) del Data_Lake_Bucket.
2. WHEN el DataLakeStack se sintetiza, THE DataLakeStack SHALL exponer exactamente un output cuyo
   valor resuelve al nombre real del Glue_Transform_Job.
3. WHEN el DataLakeStack se sintetiza, THE DataLakeStack SHALL exponer exactamente un output cuyo
   valor resuelve al nombre real del Athena_Workgroup.
4. WHEN el DataLakeStack se sintetiza, THE DataLakeStack SHALL exponer exactamente un output cuyo
   valor resuelve al nombre real de la base de datos `datalake_db`.
5. WHEN el DataLakeStack se sintetiza, THE DataLakeStack SHALL exponer los cuatro outputs anteriores
   y ningún otro output adicional.
6. THE DataLakeStack SHALL asignar a cada output una descripción en español, no vacía y única.
7. IF un recurso referenciado por un output no existe en el momento de la síntesis, THEN THE
   DataLakeStack SHALL interrumpir la síntesis sin generar un template parcial e indicar el recurso
   ausente.

### Requisito 9: Generador de datos de ejemplo

**Historia de usuario:** Como usuario que prueba el pipeline, quiero un script que genere un CSV de
ventas ficticias, para tener datos listos que subir a la zona raw sin construirlos a mano.

#### Criterios de aceptación

1. THE DataLakeStack SHALL incluir un Data_Generator standalone en
   `sample_data/generate_ventas.py` ejecutable de forma independiente de la infraestructura, sin
   requerir credenciales de AWS ni recursos desplegados.
2. WHEN el Data_Generator se ejecuta, THE Data_Generator SHALL producir un archivo CSV de ventas
   ficticias con exactamente las columnas `fecha`, `sucursal`, `producto`, `cantidad` y `monto`, en
   ese orden, incluyendo una fila de encabezado con esos nombres.
3. WHEN el Data_Generator se ejecuta, THE Data_Generator SHALL generar entre 100 y 1000 filas de
   datos, donde `fecha` tiene formato `AAAA-MM-DD`, `cantidad` es un entero entre 1 y 1000, y
   `monto` es un decimal entre 0.01 y 999999.99 con dos decimales.
4. THE archivo CSV generado SHALL estar codificado en UTF-8 y usar la coma como separador de
   campos, de modo que sea apto para subirse al prefijo `raw/ventas/` del Data_Lake_Bucket.
5. IF el Data_Generator no puede escribir el archivo CSV en la ruta de destino, THEN THE
   Data_Generator SHALL terminar con un código de salida distinto de cero y emitir un mensaje de
   error que indique la causa del fallo, sin dejar un archivo CSV parcial.

### Requisito 10: Validación del template con CDK assertions

**Historia de usuario:** Como data engineer, quiero tests automatizados que validen las propiedades
de seguridad del stack, para garantizar que la configuración crítica no se rompa al evolucionar el
código.

#### Criterios de aceptación

1. WHEN se ejecuta la CDK_Test_Suite, THE CDK_Test_Suite SHALL ejecutar sus aserciones contra el
   template sintetizado mediante `aws_cdk.assertions.Template`, sin desplegar recursos reales en
   AWS.
2. WHEN la CDK_Test_Suite valida el Data_Lake_Bucket, THE CDK_Test_Suite SHALL verificar que el
   bucket tiene cifrado en reposo configurado.
3. WHEN la CDK_Test_Suite valida el Data_Lake_Bucket, THE CDK_Test_Suite SHALL verificar que las
   cuatro restricciones de bloqueo de acceso público están en valor verdadero.
4. WHEN la CDK_Test_Suite valida la notificación de eventos del Data_Lake_Bucket, THE
   CDK_Test_Suite SHALL verificar que existe exactamente un filtro de prefijo con valor `raw/`.
5. WHEN la CDK_Test_Suite valida los roles IAM del DataLakeStack, THE CDK_Test_Suite SHALL
   verificar que ninguno de los roles incluye declaraciones con recursos `Resource: "*"`.
6. IF alguna de las propiedades de seguridad verificadas no coincide con el valor esperado, THEN
   THE CDK_Test_Suite SHALL fallar el test correspondiente e indicar la propiedad que no cumple.

### Requisito 11: Parametrización de cuenta y región

**Historia de usuario:** Como usuario que despliega el stack en su propia cuenta, quiero que la
cuenta y la región se tomen del entorno, para desplegar donde quiera sin editar el código.

#### Criterios de aceptación

1. WHEN se instancia el DataLakeStack, THE DataLakeStack SHALL obtener el identificador de cuenta y
   la región de despliegue desde el environment de CDK, sin que ningún valor de cuenta o región
   figure literal (hardcodeado) en el código fuente del stack.
2. WHERE no se especifica una región en el environment, THE DataLakeStack SHALL usar una región por
   defecto provista mediante un parámetro configurable externo al código del stack (variable de
   environment o contexto de CDK), de modo que cambiar la región por defecto no requiera modificar
   el código fuente.
3. IF no es posible determinar la cuenta ni la región a partir del environment de CDK ni del
   parámetro por defecto configurable, THEN THE DataLakeStack SHALL detener la síntesis sin crear
   recursos y devolver un error que indique cuál de los dos valores (cuenta o región) falta.
