"""Tests de propiedad (Hypothesis) para el generador de datos de ejemplo.

Cubre la **Property 4** del diseño: para cualquier ejecución del Data_Generator
(``sample_data/generate_ventas.py``) con cualquier semilla aleatoria, el CSV producido
cumple el esquema y los rangos del Requisito 9 (columnas exactas y en orden, entre 100 y
1000 filas de datos, ``fecha`` en formato AAAA-MM-DD, ``cantidad`` entero 1–1000,
``monto`` decimal 0.01–999999.99 con dos decimales) y está codificado en UTF-8 con coma
como separador de campos.

Sobre la importación del módulo: ``sample_data/`` no es un paquete importable (no tiene
``__init__.py``). El ``conftest.py`` de la suite agrega ese directorio al ``sys.path``
para poder hacer ``import generate_ventas`` sin manipular rutas en cada archivo. El
generador usa solo la librería estándar, por lo que no se realiza ninguna llamada a AWS.

Estrategia de prueba: se invoca ``main(["--semilla", <seed>, "-o", <tmpfile>])`` **sin**
``--filas``, de modo que se ejercite el camino del conteo aleatorio de filas (100–1000);
luego se leen los bytes del archivo, se valida la codificación UTF-8 y el separador coma,
y se parsea con el módulo ``csv`` estándar para verificar el esquema y los rangos.

Cada test ejecuta un mínimo de 100 ejemplos (``@settings(max_examples=100)``) e implementa
una única propiedad de corrección. Se usa ``deadline=None`` porque cada ejemplo escribe y
relee un archivo en disco, lo que puede ser lento.
"""

import csv
import datetime
import os
import re
import tempfile

from hypothesis import given, settings
from hypothesis import strategies as st

import generate_ventas
from generate_ventas import COLUMNAS, MAX_FILAS, MIN_FILAS

# ``monto`` debe ser un decimal con EXACTAMENTE dos decimales: una o más cifras enteras,
# punto y dos cifras decimales.
_PATRON_MONTO = re.compile(r"^\d+\.\d{2}$")


# Feature: serverless-datalake-aws, Property 4: El CSV generado siempre cumple el esquema
# y los rangos (columnas exactas y en orden, 100–1000 filas, fecha AAAA-MM-DD, cantidad
# entero 1–1000, monto decimal 0.01–999999.99 con dos decimales, UTF-8 + coma).
@settings(max_examples=100, deadline=None)
@given(semilla=st.integers(min_value=0, max_value=2**31 - 1))
def test_csv_generado_cumple_esquema_y_rangos(semilla):
    """Para cualquier semilla, el CSV producido por ``main`` tiene exactamente las
    columnas requeridas en orden, entre 100 y 1000 filas de datos, y cada valor respeta
    su formato y rango; además está en UTF-8 con coma como separador."""
    with tempfile.TemporaryDirectory() as directorio:
        ruta = os.path.join(directorio, "ventas.csv")

        # Se invoca sin --filas para ejercitar el camino del conteo aleatorio (100–1000).
        codigo = generate_ventas.main(["--semilla", str(semilla), "-o", ruta])

        # La ejecución debe ser exitosa y dejar el archivo en la ruta indicada.
        assert codigo == 0
        assert os.path.exists(ruta)

        # --- Codificación UTF-8 y separador coma -------------------------------------
        # Leer los bytes crudos y decodificar como UTF-8: si no fuera UTF-8 válido, esto
        # lanzaría UnicodeDecodeError y el test fallaría (Requisito 9.4).
        with open(ruta, "rb") as binario:
            contenido = binario.read().decode("utf-8")

        # La primera línea (encabezado) debe usar la coma como separador y contener
        # exactamente las columnas requeridas, en orden (Requisito 9.2, 9.4).
        primera_linea = contenido.splitlines()[0]
        assert primera_linea == ",".join(COLUMNAS)

        # --- Parseo con el módulo csv estándar ---------------------------------------
        with open(ruta, "r", encoding="utf-8", newline="") as archivo:
            lector = csv.reader(archivo)
            encabezado = next(lector)
            # Columnas exactas y en el orden requerido (Requisito 9.2).
            assert encabezado == COLUMNAS
            filas = list(lector)

        # --- Conteo de filas de datos (sin contar el encabezado) ---------------------
        assert MIN_FILAS <= len(filas) <= MAX_FILAS

        # --- Validación de cada fila --------------------------------------------------
        for fila in filas:
            # Cada fila tiene exactamente cinco campos (uno por columna).
            assert len(fila) == len(COLUMNAS)
            fecha, sucursal, producto, cantidad, monto = fila

            # ``fecha`` con formato AAAA-MM-DD real (Requisito 9.3).
            datetime.date.fromisoformat(fecha)
            assert re.match(r"^\d{4}-\d{2}-\d{2}$", fecha)

            # ``sucursal`` y ``producto`` no vacíos (esquema lógico del CSV).
            assert sucursal
            assert producto

            # ``cantidad`` entero entre 1 y 1000 (Requisito 9.3).
            valor_cantidad = int(cantidad)
            assert 1 <= valor_cantidad <= 1000

            # ``monto`` decimal con dos decimales entre 0.01 y 999999.99 (Requisito 9.3).
            assert _PATRON_MONTO.match(monto)
            valor_monto = float(monto)
            assert 0.01 <= valor_monto <= 999999.99
