from datetime import datetime
from os import system as os_system
from platform import system as platform_system
from re import fullmatch
import tomllib
from pathlib import Path

from .constants import CONFIG_FILE


def clear_screen():
    system = platform_system()

    if system == "Linux":
        os_system("clear")
    elif system == "Windows":
        os_system("cls")


def get_time_now() -> str:
    """
    Devuelve la fecha y hora actual con el formato YYYY-mm-dd HH:MM:SS
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_config() -> dict:
    """
    Devuelve un diccionario con la configuración del archivo de configuración
    """
    path = Path(CONFIG_FILE).expanduser()

    if not path.is_absolute():
        path = Path.home() / CONFIG_FILE

    with open(path, "rb") as file:
        data = tomllib.load(file)

    return data


def verify_number(string: str) -> float:
    """
    Verifica y convierte un dato de tipo `str` a un dato de tipo `float`
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

    return 0.0


def clinput(s: str = "") -> str:
    return input(s).strip(" /:")
