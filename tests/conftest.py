"""Configuración compartida de pytest para la suite de tests.

Por qué: tanto el script del Glue Job (``glue_src/``) como el código de la Lambda
(``lambda_src/``) viven como assets separados de la infraestructura y, por convención
del proyecto, esos directorios no son paquetes importables (no tienen ``__init__.py``).
Para poder importar ``transform_job`` y ``trigger_pipeline`` desde los tests, ambos
directorios se agregan al ``sys.path`` aquí, de modo que cualquier test pueda hacer
``import transform_job`` o ``import trigger_pipeline`` sin manipular rutas en cada
archivo. Lo mismo aplica al generador de datos de ejemplo (``sample_data/``), que
tampoco es un paquete importable, para poder hacer ``import generate_ventas``.

Además se fija una región AWS por defecto (``AWS_DEFAULT_REGION``) *antes* de que se
importen los módulos de test. Esto es necesario porque ``lambda_src/trigger_pipeline.py``
crea un ``boto3.client("glue")`` en tiempo de importación; sin una región configurada,
boto3 abortaría con ``NoRegionError`` al intentar importar el módulo en un entorno sin
credenciales/perfil AWS (p. ej. en CI). No se realiza ninguna llamada real a AWS: solo
se construye el cliente, por lo que una región ficticia es suficiente.
"""

import os
import sys

# Región AWS por defecto para los tests. Se fija solo si el entorno no define ya una,
# para no pisar la configuración real de quien ejecute la suite localmente.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

# Raíz del repositorio (un nivel por encima de ``tests/``).
_RAIZ_PROYECTO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# La raíz del repositorio también se agrega al path para poder importar ``app`` (el
# punto de entrada de CDK vive en ``app.py`` en la raíz, no dentro de un paquete).
# Importar ``app`` no ejecuta ``main()`` porque está protegido por ``__main__``.
if _RAIZ_PROYECTO not in sys.path:
    sys.path.insert(0, _RAIZ_PROYECTO)

# Directorios de assets de runtime. Se insertan al inicio del path para que
# ``import transform_job`` e ``import trigger_pipeline`` resuelvan a los módulos
# correctos.
for _asset in ("glue_src", "lambda_src", "sample_data"):
    _ruta = os.path.join(_RAIZ_PROYECTO, _asset)
    if _ruta not in sys.path:
        sys.path.insert(0, _ruta)
