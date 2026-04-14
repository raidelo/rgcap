from datetime import datetime
from os import system as os_system
from platform import system as platform_system
from re import fullmatch
from pathlib import Path

import tomllib

from .constants import CONFIG_FILE


def load_config() -> dict[str, str]:
    """
    Devuelve un diccionario con la configuración del archivo de configuración
    """

    path = Path(CONFIG_FILE).expanduser()

    if not path.is_absolute():
        path = Path.home() / CONFIG_FILE

    with open(path, "rb") as file:
        data = tomllib.load(file)

    return data


def clear_screen():
    """
    Intenta limpiar la terminal. (Solo para sistemas Windows y Linux)
    """
    system = platform_system()

    if system == "Windows":
        os_system("cls")
    elif system == "Linux":
        os_system("clear")


def get_time_now(format: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Devuelve la fecha y hora actual con el formato dado.

    Por defecto: YYYY-mm-dd HH:MM:SS
    """

    return datetime.now().strftime(format)


def float_from_str(string: str) -> float:
    """
    Intenta convertir un dato de tipo `str` a un dato de tipo `float`

    Si no se puede, devuelve None
    """

    sign = "[+-]?"
    number = "\\d+"
    dot_or_comma = "[\\.\\,]?"

    matches = fullmatch(
        "%(sign)s(%(number)s%(dot_or_comma)s|%(dot_or_comma)s%(number)s|%(number)s%(dot_or_comma)s%(number)s)"
        % {
            "sign": sign,
            "number": number,
            "dot_or_comma": dot_or_comma,
        },
        string,
    )

    if matches:
        try:
            matches = string[matches.start() : matches.end()].replace(",", ".")
            return float(matches)
        except ValueError:
            pass

    return None


def clinput(s: str = "") -> str:
    return input(s).strip(" /:")
