"""Unit tests (casos de error) del generador de datos ``generate_ventas``.

Cubre el Requisito 9.5: cuando la ruta de destino del CSV no es escribible, el generador
debe terminar con un código de salida distinto de cero, emitir en ``stderr`` un mensaje
que indique la causa del fallo y **no** dejar un archivo CSV parcial en disco.

Se ejercitan dos niveles:

- ``main(argv)`` de punta a punta con una ruta dentro de un directorio inexistente: se
  comprueba el código de salida, la ausencia de archivo y el mensaje de causa en stderr.
- ``escribir_csv`` directamente: se comprueba que lanza ``OSError`` ante un destino no
  escribible y que no deja el archivo de destino creado.

No se usan mocks: el fallo es real (un directorio que no existe), por lo que el test
valida el comportamiento verdadero del manejo de errores de escritura.
"""

import os

import pytest

from generate_ventas import escribir_csv, generar_filas


# --------------------------------------------------------------------------------------
# Requisito 9.5: ruta no escribible -> exit != 0, causa en stderr y sin archivo parcial
# --------------------------------------------------------------------------------------


def test_main_ruta_no_escribible_retorna_error_sin_archivo_parcial(tmp_path, capsys):
    """``main`` falla con código 1 y sin dejar archivo cuando el directorio no existe.

    Verifica el Requisito 9.5 de punta a punta: se apunta la salida a un archivo dentro de
    un subdirectorio que no existe (``<tmp>/no_existe/ventas.csv``). El generador debe
    terminar con código distinto de cero (1, reservado para errores de escritura), no debe
    crear ningún archivo en esa ruta (sin archivo parcial) y debe emitir en ``stderr`` la
    causa del fallo.
    """
    # Ruta de destino dentro de un directorio inexistente: la escritura no es posible.
    ruta_destino = tmp_path / "no_existe" / "ventas.csv"

    # Se fijan fecha, filas y semilla para que el test sea determinista; el único motivo
    # de fallo posible es la ruta no escribible.
    codigo = main_argv(
        [
            "-o",
            str(ruta_destino),
            "--fecha",
            "2025-06-10",
            "--filas",
            "100",
            "--semilla",
            "7",
        ]
    )

    # Código de salida distinto de cero (concretamente 1 para errores de escritura).
    assert codigo == 1

    # No debe existir ningún archivo en la ruta de destino (sin archivo parcial).
    assert not ruta_destino.exists()
    # El directorio inexistente tampoco debe haberse creado de forma colateral.
    assert not ruta_destino.parent.exists()

    # El mensaje de causa debe haberse emitido por stderr (no por stdout).
    capturado = capsys.readouterr()
    assert capturado.err.strip() != ""
    assert "No se pudo escribir el CSV" in capturado.err
    assert str(ruta_destino) in capturado.err


def test_escribir_csv_destino_no_escribible_lanza_oserror_sin_archivo(tmp_path):
    """``escribir_csv`` lanza ``OSError`` y no crea el archivo si el directorio no existe.

    Verifica el Requisito 9.5 a nivel de función: ``escribir_csv`` debe fallar con
    ``OSError`` (el temporal se crea con ``mkstemp`` en el directorio de destino, que no
    existe) y la ruta de destino debe quedar sin crear, garantizando que no hay archivo
    parcial.
    """
    ruta_destino = tmp_path / "tampoco_existe" / "ventas.csv"
    filas = generar_filas(100, "2025-06-10", _rng_determinista())

    with pytest.raises(OSError):
        escribir_csv(filas, str(ruta_destino))

    # No debe haberse creado el archivo de destino ni su directorio.
    assert not ruta_destino.exists()
    assert not ruta_destino.parent.exists()


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


def main_argv(argv):
    """Invoca ``generate_ventas.main`` con ``argv`` y devuelve su código de salida.

    Por qué un wrapper: aísla la importación de ``main`` en un solo lugar y deja explícito
    en cada test que se está ejecutando el punto de entrada de línea de comandos.
    """
    from generate_ventas import main

    return main(argv)


def _rng_determinista():
    """Devuelve un ``random.Random`` con semilla fija para filas reproducibles."""
    import random

    return random.Random(7)
