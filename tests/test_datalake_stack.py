"""CDK assertions del template sintetizado del ``DataLakeStack``.

Por qué: la mayor parte del proyecto es Infraestructura como Código, cuya
corrección de configuración se valida mejor con *assertions* sobre el template
sintetizado en memoria (Requisito 10.1), no con property-based testing ni
desplegando recursos reales en AWS. Aquí se sintetiza el stack una sola vez (vía
una fixture de pytest) y se verifican las propiedades de seguridad y de
configuración estática que no deben romperse al evolucionar el código.

Cobertura de este archivo (design.md sección 10 y Requisitos 10.1–10.6):

- Cifrado en reposo del bucket (Requisito 10.2).
- Las cuatro flags de bloqueo de acceso público en ``true`` (Requisito 10.3).
- Exactamente un filtro de prefijo con valor ``raw/`` en la notificación
  (Requisito 10.4).
- Configuración estática restante: runtime/timeout de la Lambda, ARN único en
  ``glue:StartJobRun``, recursos del Glue Job Role (raw/processed sin curated),
  workgroup de Athena (enforce + 1 GiB + athena-results/), crawler sin schedule,
  location de Lake Formation con DependsOn al bucket y Analyst_Role sin política
  IAM directa sobre S3.

Las Properties 6 (ningún rol IAM con ``Resource: "*"``) y 7 (outputs con
descripción no vacía y única) se implementan en tareas separadas (11.2 y 11.3),
por lo que NO se cubren en este archivo.

Cómo se sintetiza el stack: se instancia ``DataLakeStack`` con un ``env``
explícito (cuenta y región ficticias) para que CDK pueda resolver tokens
dependientes de cuenta/región sin necesidad de credenciales reales; luego
``Template.from_stack`` produce el template en memoria, sin desplegar nada
(Requisito 10.1).
"""

import json

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template

from datalake.datalake_stack import DataLakeStack

# Cuenta y región ficticias: solo se usan para resolver tokens del template; no
# se realiza ninguna llamada real a AWS ni se despliega nada.
CUENTA_PRUEBA = "123456789012"
REGION_PRUEBA = "us-east-1"


@pytest.fixture(scope="module")
def template() -> Template:
    """Sintetiza el ``DataLakeStack`` en memoria una sola vez por módulo.

    Por qué scope ``module``: la síntesis es relativamente costosa y el template
    resultante es inmutable, así que construirlo una vez y reutilizarlo en todos
    los tests del archivo acelera la suite sin afectar el aislamiento (ningún
    test muta el template).
    """
    app = cdk.App()
    stack = DataLakeStack(
        app,
        "TestDataLakeStack",
        env=cdk.Environment(account=CUENTA_PRUEBA, region=REGION_PRUEBA),
    )
    return Template.from_stack(stack)


def _documentos_de_politica(template: Template) -> list[dict]:
    """Devuelve los ``PolicyDocument`` de todos los ``AWS::IAM::Policy``.

    Por qué: varias aserciones necesitan inspeccionar las sentencias IAM crudas
    (acción ``glue:StartJobRun``, recursos del Glue Job Role), y resulta más
    claro recorrer los documentos directamente sobre el JSON del template que
    encadenar matchers.
    """
    policies = template.find_resources("AWS::IAM::Policy")
    return [p["Properties"]["PolicyDocument"] for p in policies.values()]


def _sentencias_con_accion(template: Template, accion: str) -> list[dict]:
    """Recolecta todas las sentencias IAM que incluyen la acción dada.

    La propiedad ``Action`` de una sentencia puede ser un string (una sola
    acción) o una lista; se normaliza a lista para comparar de forma uniforme.
    """
    encontradas: list[dict] = []
    for documento in _documentos_de_politica(template):
        for sentencia in documento.get("Statement", []):
            acciones = sentencia.get("Action", [])
            if isinstance(acciones, str):
                acciones = [acciones]
            if accion in acciones:
                encontradas.append(sentencia)
    return encontradas


def test_cifrado_en_reposo_del_bucket(template: Template) -> None:
    """Requisito 10.2: el bucket tiene cifrado en reposo configurado.

    Se exige que exista la configuración de cifrado del lado del servidor con un
    algoritmo definido; no importa el algoritmo concreto, solo que el cifrado en
    reposo esté presente.
    """
    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "ServerSideEncryptionByDefault": Match.object_like(
                                    {"SSEAlgorithm": Match.any_value()}
                                )
                            }
                        )
                    ]
                )
            }
        },
    )


def test_bloqueo_total_de_acceso_publico(template: Template) -> None:
    """Requisito 10.3: las cuatro flags de bloqueo de acceso público en ``true``.

    BlockPublicAcls, IgnorePublicAcls, BlockPublicPolicy y RestrictPublicBuckets
    deben estar todas en verdadero para que ninguna ACL o política pueda abrir el
    bucket al público.
    """
    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            }
        },
    )


def test_un_solo_filtro_de_prefijo_raw_en_la_notificacion(
    template: Template,
) -> None:
    """Requisito 10.4: exactamente un filtro de prefijo con valor ``raw/``.

    La notificación de S3 se materializa como recurso ``Custom::S3BucketNotifications``.
    Se recorren todas las reglas de filtro de todas las configuraciones de Lambda
    y se cuentan las reglas de tipo *prefix* cuyo valor es ``raw/``; debe haber
    exactamente una. Esto evita que un objeto escrito en otra zona (p. ej.
    ``processed/``) dispare la Lambda y provoque un loop.
    """
    notificaciones = template.find_resources("Custom::S3BucketNotifications")
    assert len(notificaciones) == 1, (
        "Se esperaba exactamente un recurso Custom::S3BucketNotifications"
    )

    (recurso,) = notificaciones.values()
    config = recurso["Properties"]["NotificationConfiguration"]
    lambda_configs = config["LambdaFunctionConfigurations"]

    reglas_prefijo_raw = []
    for lambda_config in lambda_configs:
        reglas = lambda_config.get("Filter", {}).get("Key", {}).get(
            "FilterRules", []
        )
        for regla in reglas:
            # El nombre de la regla en el template es "prefix" (minúscula); se
            # compara sin distinguir mayúsculas para ser robustos.
            if regla.get("Name", "").lower() == "prefix" and regla.get(
                "Value"
            ) == "raw/":
                reglas_prefijo_raw.append(regla)

    assert len(reglas_prefijo_raw) == 1, (
        "Debe existir exactamente un filtro de prefijo con valor 'raw/', "
        f"se encontraron {len(reglas_prefijo_raw)}"
    )


def test_lambda_runtime_y_timeout(template: Template) -> None:
    """Requisito 2.1 (config estática): la Trigger_Lambda usa Python 3.12 y 60 s.

    Se verifica conjuntamente runtime y timeout para identificar la Trigger_Lambda
    de forma inequívoca, ya que el template incluye además Lambdas de recursos
    personalizados (con otros runtimes/timeouts) que no deben confundirse con la
    función del pipeline.
    """
    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Runtime": "python3.12",
            "Timeout": 60,
        },
    )


def test_startjobrun_con_arn_unico_y_sin_wildcard(template: Template) -> None:
    """Requisito 7.1/7.4 (config estática): ``glue:StartJobRun`` apunta a un único ARN.

    Debe existir exactamente una sentencia con la acción ``glue:StartJobRun`` y su
    ``Resource`` debe ser un único ARN concreto del Glue Job (referenciado por
    CloudFormation), nunca ``"*"`` ni una lista de varios recursos.
    """
    sentencias = _sentencias_con_accion(template, "glue:StartJobRun")
    assert len(sentencias) == 1, (
        "Debe haber exactamente una sentencia con la accion glue:StartJobRun"
    )

    recurso = sentencias[0]["Resource"]
    # Un único recurso: no debe ser una lista de varios ARNs.
    assert not isinstance(recurso, list), (
        "glue:StartJobRun debe referenciar un unico ARN, no una lista"
    )
    # No debe ser el comodín "*".
    assert recurso != "*", "glue:StartJobRun no debe usar Resource '*'"
    # El ARN concreto referencia el Glue Job ('job/' aparece en el Fn::Join).
    assert "job/" in json.dumps(recurso), (
        "El ARN de glue:StartJobRun debe referenciar un Glue Job concreto"
    )


def test_glue_job_role_incluye_raw_processed_sin_curated(
    template: Template,
) -> None:
    """Requisito 7.2/7.5 (config estática): el Glue Job Role toca raw/ y processed/, no curated/.

    Se localiza el documento de política del rol del Glue Job (identificado por la
    sentencia ``ReadRawObjects``) y se comprueba que sus recursos mencionan las
    zonas ``raw/`` y ``processed/`` pero en ningún caso ``curated``.
    """
    documento_job = None
    for documento in _documentos_de_politica(template):
        sids = {s.get("Sid") for s in documento.get("Statement", [])}
        if "ReadRawObjects" in sids:
            documento_job = documento
            break

    assert documento_job is not None, (
        "No se encontro el documento de politica del Glue Job Role"
    )

    serializado = json.dumps(documento_job)
    assert "raw/" in serializado, "El Glue Job Role debe conceder acceso a raw/"
    assert "processed/" in serializado, (
        "El Glue Job Role debe conceder acceso a processed/"
    )
    assert "curated" not in serializado, (
        "El Glue Job Role NO debe referenciar la zona curated/"
    )


def test_athena_workgroup_proteccion_de_costos(template: Template) -> None:
    """Requisito 5.2/5.3/5.4 (config estática): workgroup con enforce, límite y salida.

    El workgroup debe forzar su configuración (``EnforceWorkGroupConfiguration``
    en ``true``), limitar el escaneo a 1 GiB (``BytesScannedCutoffPerQuery`` =
    1.073.741.824 bytes) y dirigir los resultados a un prefijo ``athena-results/``.
    """
    workgroups = template.find_resources("AWS::Athena::WorkGroup")
    assert len(workgroups) == 1, "Se esperaba un unico Athena WorkGroup"

    (workgroup,) = workgroups.values()
    config = workgroup["Properties"]["WorkGroupConfiguration"]

    assert config["EnforceWorkGroupConfiguration"] is True
    assert config["BytesScannedCutoffPerQuery"] == 1_073_741_824
    salida = json.dumps(config["ResultConfiguration"]["OutputLocation"])
    assert "athena-results/" in salida, (
        "La ubicacion de resultados debe estar bajo athena-results/"
    )


def test_crawler_sin_schedule(template: Template) -> None:
    """Requisito 4.6 (config estática): el crawler no tiene schedule.

    Sin ``Schedule`` el crawler solo se ejecuta por el trigger condicional (job
    SUCCEEDED) o bajo demanda, nunca por tiempo.
    """
    crawlers = template.find_resources("AWS::Glue::Crawler")
    assert len(crawlers) == 1, "Se esperaba un unico Glue Crawler"

    (crawler,) = crawlers.values()
    assert "Schedule" not in crawler["Properties"], (
        "El crawler no debe definir un Schedule"
    )


def test_lakeformation_location_depende_del_bucket(template: Template) -> None:
    """Requisito 6.5 (config estática): la location de Lake Formation depende del bucket.

    El registro del bucket como location de Lake Formation solo referencia el ARN
    del bucket (un atributo), por lo que CDK no infiere la dependencia: se declara
    explícitamente y debe figurar en el ``DependsOn`` del recurso de location.
    """
    buckets = template.find_resources("AWS::S3::Bucket")
    assert len(buckets) == 1, "Se esperaba un unico bucket S3"
    (id_logico_bucket,) = buckets.keys()

    locations = template.find_resources("AWS::LakeFormation::Resource")
    assert len(locations) == 1, "Se esperaba un unico Lake Formation Resource"
    (location,) = locations.values()

    depends_on = location.get("DependsOn", [])
    if isinstance(depends_on, str):
        depends_on = [depends_on]
    assert id_logico_bucket in depends_on, (
        "La location de Lake Formation debe declarar DependsOn al bucket"
    )


def test_analyst_role_sin_politica_s3_inline(template: Template) -> None:
    """Requisito 6.2 (config estática): el Analyst_Role no concede S3 por IAM.

    El acceso del analista a los datos se gobierna exclusivamente vía Lake
    Formation (permiso SELECT), no por IAM. Por tanto el rol no debe tener
    políticas inline (``Policies``) ni políticas administradas adjuntas que
    concedan acciones de S3.
    """
    roles = template.find_resources("AWS::IAM::Role")
    analyst_roles = [
        rol
        for rol in roles.values()
        if "analista" in (rol["Properties"].get("Description") or "").lower()
    ]
    assert len(analyst_roles) == 1, (
        "Se esperaba un unico rol de analista identificable por su descripcion"
    )

    propiedades = analyst_roles[0]["Properties"]
    # No debe haber políticas inline en el rol del analista.
    politicas_inline = propiedades.get("Policies", [])
    serializado = json.dumps(politicas_inline)
    assert "s3:" not in serializado, (
        "El Analyst_Role no debe conceder acciones de S3 mediante politicas inline"
    )
    # Tampoco debe adjuntar políticas administradas (que podrían dar S3).
    assert not propiedades.get("ManagedPolicyArns"), (
        "El Analyst_Role no debe adjuntar politicas administradas"
    )
