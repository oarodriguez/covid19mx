"""Project configuration parameters."""
from __future__ import annotations

from pathlib import Path

from attr import dataclass

# NOTE: This path assumes the source code lies in the project directory
PROJECT_PATH = Path(__file__).parent.parent.parent

ROOT_DATA_SOURCE_URL = (
    "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos"
)
ZIPPED_COVID_DATA_FILENAME = "datos_abiertos_covid19.zip"
ZIPPED_DATA_DICTIONARY_FILENAME = "diccionario_datos_covid19.zip"

COVID_DATA_URL = f"{ROOT_DATA_SOURCE_URL}/{ZIPPED_COVID_DATA_FILENAME}"
DATA_DICTIONARY_URL = (
    f"{ROOT_DATA_SOURCE_URL}/{ZIPPED_DATA_DICTIONARY_FILENAME}"
)

DATA_DIR_NAME = "data"
DATA_PATH = PROJECT_PATH / DATA_DIR_NAME

COVID_DATA_FILE = DATA_PATH / ZIPPED_COVID_DATA_FILENAME
DATA_DICTIONARY_FILE = DATA_PATH / ZIPPED_DATA_DICTIONARY_FILENAME


@dataclass
class Config:
    """Represent the global configuration of the project."""

    # The project root path.
    root_path: Path

    # Path used to store data.
    data_path: Path

    # The remote URL pointing to the COVID compressed data.
    covid_data_url: str

    # The remote URL pointing to the COVID compressed data dictionary.
    data_dictionary_url: str

    # The filename we assign to the downloaded COVID compressed data file.
    zipped_covid_data_filename: str

    # The filename we assign to the downloaded compressed dictionary data file.
    zipped_data_dictionary_filename: str

    @classmethod
    def default(cls):
        """Return a new configuration instance with the default attributes."""
        return cls(
            PROJECT_PATH,
            DATA_PATH,
            COVID_DATA_URL,
            DATA_DICTIONARY_URL,
            ZIPPED_COVID_DATA_FILENAME,
            ZIPPED_DATA_DICTIONARY_FILENAME,
        )
