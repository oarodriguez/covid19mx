"""Project configuration parameters."""
from __future__ import annotations

from pathlib import Path

# NOTE: This path assumes the source code lies in the project directory
PROJECT_PATH = Path(__file__).parent.parent.parent

BASE_DATA_SOURCE_URL = (
    "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos"
)
COVID_DATA_FILENAME = "datos_abiertos_covid19.zip"
DATA_DICTIONARY_FILENAME = "diccionario_datos_covid19.zip"

COVID_DATA_URL = f"{BASE_DATA_SOURCE_URL}/{COVID_DATA_FILENAME}"
DATA_DICTIONARY_URL = f"{BASE_DATA_SOURCE_URL}/{DATA_DICTIONARY_FILENAME}"

DATA_DIR_NAME = "data"
DATA_DIR_PATH = PROJECT_PATH / DATA_DIR_NAME

COVID_DATA_FILE_PATH = DATA_DIR_PATH / COVID_DATA_FILENAME
DATA_DICTIONARY_FILE_PATH = DATA_DIR_PATH / DATA_DICTIONARY_FILENAME
