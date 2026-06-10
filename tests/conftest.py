"""Configuración compartida de pytest para la suite de tests.

Por qué: el script del Glue Job vive en ``glue_src/`` como asset separado de la
infraestructura y, por convención del proyecto, ese directorio no es un paquete
importable (no tiene ``__init__.py``). Para poder importar ``transform_job`` desde
los tests, se agrega ``glue_src/`` al ``sys.path`` aquí, de modo que cualquier test
pueda hacer ``import transform_job`` sin manipular rutas en cada archivo.
"""

import os
import sys

# Raíz del repositorio (un nivel por encima de ``tests/``).
_RAIZ_PROYECTO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Directorio del script del Glue Job. Se inserta al inicio del path para que
# ``import transform_job`` resuelva al módulo correcto.
_GLUE_SRC = os.path.join(_RAIZ_PROYECTO, "glue_src")
if _GLUE_SRC not in sys.path:
    sys.path.insert(0, _GLUE_SRC)
