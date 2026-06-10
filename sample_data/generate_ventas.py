"""Generador standalone de un CSV de ventas ficticias para la Zona Raw.

Por qué existe: para probar el pipeline de punta a punta hace falta un archivo de
ventas con datos realistas que subir a ``raw/ventas/``. Este script lo genera sin
construirlo a mano, **sin depender de AWS** ni de credenciales: usa exclusivamente la
librería estándar (``csv``, ``random``, ``datetime``, ``argparse``). No importa boto3 ni
pandas (Requisito 9.1).

Diseño orientado a funciones puras y testeables:

- ``generar_filas``            : devuelve las filas de datos (lógica pura, sin E/S).
- ``escribir_csv``             : escritura atómica del CSV vía archivo temporal + rename.
- ``nombre_archivo_por_defecto`` : aplica la convención ``ventas-AAAA-MM-DD.csv``.
- ``main``                     : interfaz de línea de comandos (argparse).

La separación permite que la task 10.2 escriba un test de propiedad contra
``generar_filas`` y que la task 10.3 pruebe ``escribir_csv`` ante una ruta no escribible,
sin necesidad de ejecutar el script completo.

Ejemplo de uso::

    python sample_data/generate_ventas.py                 # ventas-<hoy>.csv
    python sample_data/generate_ventas.py -o ventas.csv   # ruta explícita
    python sample_data/generate_ventas.py --fecha 2025-06-10 --filas 250 --semilla 7
"""

import argparse
import csv
import datetime
import os
import random
import sys
import tempfile

# Columnas exactas y en el orden requerido, con fila de encabezado (Requisito 9.2).
COLUMNAS = ["fecha", "sucursal", "producto", "cantidad", "monto"]

# Rango de filas de datos permitido (Requisito 9.3).
MIN_FILAS = 100
MAX_FILAS = 1000

# Rangos de los valores numéricos (Requisito 9.3).
CANTIDAD_MIN = 1
CANTIDAD_MAX = 1000
# ``monto`` se genera en centavos enteros para garantizar exactamente dos decimales y el
# rango [0.01, 999999.99]; 1 centavo = 0.01 y 99_999_999 centavos = 999999.99.
MONTO_CENTAVOS_MIN = 1
MONTO_CENTAVOS_MAX = 99_999_999

# Catálogos ficticios para que los datos sean legibles (valores no vacíos).
SUCURSALES = [
    "Buenos Aires",
    "Cordoba",
    "Rosario",
    "Mendoza",
    "La Plata",
    "Mar del Plata",
    "Salta",
    "Santa Fe",
]
PRODUCTOS = [
    "Cafe",
    "Te",
    "Azucar",
    "Harina",
    "Aceite",
    "Arroz",
    "Fideos",
    "Yerba",
    "Galletitas",
    "Mermelada",
]


def generar_filas(
    num_filas: int,
    fecha: str,
    rng: random.Random,
) -> list[dict]:
    """Genera ``num_filas`` filas de ventas ficticias para una fecha dada.

    Por qué es lógica pura (sin E/S): recibe el generador aleatorio inyectado para ser
    determinista y testeable, y devuelve estructuras en memoria. Cada fila cumple el
    esquema y los rangos del Requisito 9.3: ``cantidad`` entero entre 1 y 1000 y ``monto``
    decimal entre 0.01 y 999999.99 con exactamente dos decimales (se genera en centavos
    enteros para evitar errores de redondeo en coma flotante).

    Args:
        num_filas: cantidad de filas de datos a generar.
        fecha: fecha de las ventas en formato ``AAAA-MM-DD`` (se aplica a todas las filas,
            ya que un archivo representa las ventas de un día).
        rng: instancia de ``random.Random`` inyectada para reproducibilidad.

    Returns:
        Lista de diccionarios, uno por fila, con las claves de ``COLUMNAS``.
    """
    filas: list[dict] = []
    for _ in range(num_filas):
        centavos = rng.randint(MONTO_CENTAVOS_MIN, MONTO_CENTAVOS_MAX)
        filas.append(
            {
                "fecha": fecha,
                "sucursal": rng.choice(SUCURSALES),
                "producto": rng.choice(PRODUCTOS),
                "cantidad": rng.randint(CANTIDAD_MIN, CANTIDAD_MAX),
                # Dos decimales garantizados al formatear los centavos como X.XX.
                "monto": f"{centavos / 100:.2f}",
            }
        )
    return filas


def escribir_csv(filas: list[dict], ruta: str) -> str:
    """Escribe las filas como CSV de forma atómica (Requisito 9.4 y 9.5).

    Por qué escritura atómica vía temporal + ``os.replace``: si la escritura falla a mitad
    de camino no debe quedar un archivo CSV parcial en la ruta de destino. Estrategia:

    1. Se crea un archivo temporal en el **mismo directorio** que el destino (para que el
       rename final sea atómico dentro del mismo sistema de archivos).
    2. Se escribe el contenido completo (encabezado + filas) en UTF-8 con coma como
       separador (Requisito 9.4).
    3. Solo si todo se escribió bien, ``os.replace`` promueve el temporal al destino.

    Ante cualquier error se elimina el temporal y se relanza la excepción, de modo que la
    ruta de destino nunca queda con datos parciales (Requisito 9.5).

    Args:
        filas: filas de datos a escribir (las generadas por ``generar_filas``).
        ruta: ruta del archivo CSV de destino.

    Returns:
        La ruta del archivo CSV escrito.

    Raises:
        OSError: si el directorio de destino no existe o no es escribible.
    """
    # Directorio donde vivirá el archivo final; el temporal se crea aquí mismo para que el
    # ``os.replace`` posterior sea una operación atómica en el mismo filesystem.
    directorio = os.path.dirname(os.path.abspath(ruta))

    # ``mkstemp`` falla aquí (antes de escribir nada) si el directorio no existe o no es
    # escribible, garantizando que no se deje un archivo parcial (Requisito 9.5).
    descriptor, ruta_temporal = tempfile.mkstemp(
        dir=directorio, prefix=".ventas_", suffix=".csv.tmp"
    )
    try:
        # ``newline=""`` evita que el módulo csv duplique los saltos de línea en Windows.
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as archivo:
            escritor = csv.DictWriter(archivo, fieldnames=COLUMNAS)
            escritor.writeheader()
            escritor.writerows(filas)
        # Promoción atómica del temporal al destino final.
        os.replace(ruta_temporal, ruta)
    except Exception:
        # Limpieza del temporal para no dejar basura ni archivos parciales.
        if os.path.exists(ruta_temporal):
            os.remove(ruta_temporal)
        raise

    return ruta


def nombre_archivo_por_defecto(fecha: str) -> str:
    """Devuelve el nombre de archivo según la convención ``ventas-AAAA-MM-DD.csv``.

    Por qué: el pipeline deriva la partición de la fecha embebida en el nombre del archivo
    (ver ``glue_src/transform_job.py``), por lo que el nombre por defecto debe seguir esa
    convención para ser apto para subirse a ``raw/ventas/`` (Requisito 9.4).

    Args:
        fecha: fecha en formato ``AAAA-MM-DD``.

    Returns:
        El nombre de archivo, p. ej. ``ventas-2025-06-10.csv``.
    """
    return f"ventas-{fecha}.csv"


def _validar_fecha(valor: str) -> str:
    """Valida que ``valor`` sea una fecha ``AAAA-MM-DD`` real y la devuelve normalizada.

    Por qué: una fecha inválida produciría un nombre de archivo del que el Glue Job no
    podría extraer una partición válida. Se valida temprano para fallar con un mensaje
    claro.
    """
    try:
        return datetime.date.fromisoformat(valor).isoformat()
    except ValueError as error:
        raise ValueError(
            f"La fecha '{valor}' no tiene el formato AAAA-MM-DD válido: {error}."
        ) from error


def _construir_parser() -> argparse.ArgumentParser:
    """Construye el parser de argumentos de línea de comandos.

    Por qué se aísla en una función: facilita testear el parseo de argumentos por separado
    de la ejecución real.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Genera un CSV de ventas ficticias listo para subir a la zona raw/ventas/ "
            "del data lake. No requiere AWS ni credenciales."
        )
    )
    parser.add_argument(
        "-o",
        "--salida",
        default=None,
        help=(
            "Ruta del archivo CSV de salida. Por defecto: ventas-<fecha>.csv en el "
            "directorio actual."
        ),
    )
    parser.add_argument(
        "-n",
        "--filas",
        type=int,
        default=None,
        help=(
            f"Cantidad de filas de datos (entre {MIN_FILAS} y {MAX_FILAS}). "
            "Por defecto: un valor aleatorio en ese rango."
        ),
    )
    parser.add_argument(
        "--fecha",
        default=None,
        help="Fecha de las ventas en formato AAAA-MM-DD. Por defecto: la fecha de hoy.",
    )
    parser.add_argument(
        "--semilla",
        type=int,
        default=None,
        help="Semilla aleatoria para reproducir la misma salida (opcional).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Punto de entrada de línea de comandos del generador.

    Resuelve los argumentos, valida la fecha y la cantidad de filas, genera las filas y
    las escribe de forma atómica. Devuelve un código de salida apto para ``sys.exit``:
    ``0`` en éxito y distinto de cero ante un error de validación o de escritura, emitiendo
    en ``stderr`` la causa del fallo y sin dejar un archivo parcial (Requisito 9.5).

    Args:
        argv: lista de argumentos (para testeo); si es ``None`` usa ``sys.argv``.

    Returns:
        Código de salida del proceso (0 = éxito, != 0 = error).
    """
    args = _construir_parser().parse_args(argv)

    # Generador aleatorio inyectable; con semilla la salida es reproducible.
    rng = random.Random(args.semilla)

    # Resolución y validación de la fecha.
    try:
        fecha = _validar_fecha(args.fecha) if args.fecha else datetime.date.today().isoformat()
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2

    # Resolución y validación de la cantidad de filas.
    if args.filas is None:
        num_filas = rng.randint(MIN_FILAS, MAX_FILAS)
    elif MIN_FILAS <= args.filas <= MAX_FILAS:
        num_filas = args.filas
    else:
        print(
            f"La cantidad de filas debe estar entre {MIN_FILAS} y {MAX_FILAS}; "
            f"se recibió {args.filas}.",
            file=sys.stderr,
        )
        return 2

    ruta = args.salida or nombre_archivo_por_defecto(fecha)
    filas = generar_filas(num_filas, fecha, rng)

    try:
        escribir_csv(filas, ruta)
    except OSError as error:
        # No se pudo escribir: código != 0, causa en stderr y sin archivo parcial.
        print(f"No se pudo escribir el CSV en '{ruta}': {error}.", file=sys.stderr)
        return 1

    print(f"CSV de ventas generado: {ruta} ({num_filas} filas, fecha {fecha}).")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
