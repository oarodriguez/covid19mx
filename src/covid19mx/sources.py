"""Routines to download and extract the source data."""
from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from zipfile import ZipFile

import polars
import requests
from attr import dataclass, field
from polars import DataType, Date, Int64, Utf8

from covid19mx.config import Config

SOURCE_COVID_DATA_SCHEMA: dict[str, DataType] = {
    "FECHA_ACTUALIZACION": Date,
    "ID_REGISTRO": Utf8,
    "ORIGEN": Int64,
    "SECTOR": Int64,
    "ENTIDAD_UM": Int64,
    "SEXO": Int64,
    "ENTIDAD_NAC": Int64,
    "ENTIDAD_RES": Int64,
    "MUNICIPIO_RES": Int64,
    "TIPO_PACIENTE": Int64,
    "FECHA_INGRESO": Date,
    "FECHA_SINTOMAS": Date,
    "FECHA_DEF": Date,
    "INTUBADO": Int64,
    "NEUMONIA": Int64,
    "EDAD": Int64,
    "NACIONALIDAD": Int64,
    "EMBARAZO": Int64,
    "HABLA_LENGUA_INDIG": Int64,
    "INDIGENA": Int64,
    "DIABETES": Int64,
    "EPOC": Int64,
    "ASMA": Int64,
    "INMUSUPR": Int64,
    "HIPERTENSION": Int64,
    "OTRA_COM": Int64,
    "CARDIOVASCULAR": Int64,
    "OBESIDAD": Int64,
    "RENAL_CRONICA": Int64,
    "TABAQUISMO": Int64,
    "OTRO_CASO": Int64,
    "TOMA_MUESTRA_LAB": Int64,
    "RESULTADO_LAB": Int64,
    "TOMA_MUESTRA_ANTIGENO": Int64,
    "RESULTADO_ANTIGENO": Int64,
    "CLASIFICACION_FINAL": Int64,
    "MIGRANTE": Int64,
    "PAIS_NACIONALIDAD": Utf8,
    "PAIS_ORIGEN": Utf8,
    "UCI": Int64,
}


@dataclass
class DataChunkInfo:
    """Represent information about a binary data chunk."""

    # Chunk size in bytes.
    chunk_size: int

    # Total size in bytes of the object we split in several chunks.
    file_size: int


@dataclass
class SourceDataHandlerConfig:
    """Define the source data handler configuration attributes."""

    # Path used to store data.
    data_path: Path

    # The remote URL pointing to the COVID compressed data location.
    covid_data_url: str

    # The remote URL pointing to the COVID compressed data dictionary location.
    data_dictionary_url: str

    # The filename we assign to the downloaded COVID compressed data file.
    zipped_covid_data_filename: str

    # The filename we assign to the downloaded compressed dictionary data file.
    zipped_data_dictionary_filename: str

    @classmethod
    def default(cls):
        """Return a new configuration instance with the default attributes."""
        config = Config.default()
        return cls(
            config.data_path,
            config.covid_data_url,
            config.data_dictionary_url,
            config.zipped_covid_data_filename,
            config.zipped_data_dictionary_filename,
        )


@dataclass
class SourceDataHandler:
    """Handle all the tasks to download and extract the data sources."""

    # The data handler configuration.
    config: SourceDataHandlerConfig

    # Path used to download and extract any temporary files.
    temp_data_path: Path

    # Location of the extracted COVID data file (CSV file).
    covid_data_file: Path = field(default=None, init=False)

    # A list containing the locations of the extracted COVID data dictionary
    # files.
    data_dictionary_files: list[Path] = field(default=None, init=False)

    @property
    def zipped_covid_data_file(self):
        """Location of the downloaded COVID data compressed file."""
        return self.temp_data_path / self.config.zipped_covid_data_filename

    @property
    def zipped_data_dictionary_file(self):
        """Location of the downloaded COVID data dictionary compressed file."""
        return (
            self.temp_data_path / self.config.zipped_data_dictionary_filename
        )

    def download_covid_data_chunks(
        self, chunk_size: int = 1024 * 1024
    ) -> Iterable[DataChunkInfo]:
        """Download the COVID data in parts.

        :param chunk_size: The size in bytes of each data part.
        """
        response = requests.head(url=self.config.covid_data_url)
        response.raise_for_status()
        content_size = int(response.headers.get("Content-Length", "0"))
        with requests.get(
            url=self.config.covid_data_url, stream=True
        ) as response:
            response.raise_for_status()
            self.zipped_covid_data_file.parent.mkdir(
                exist_ok=True, parents=True
            )
            with self.zipped_covid_data_file.open("wb") as file:
                chunk_content: bytes
                for chunk_index, chunk_content in enumerate(
                    response.iter_content(chunk_size)
                ):
                    file.write(chunk_content)
                    yield DataChunkInfo(
                        chunk_size=len(chunk_content),
                        file_size=content_size,
                    )

    def download_data_dictionary(self):
        """Download the COVID data dictionaries."""
        print("Downloading dictionary data...")
        self.zipped_data_dictionary_file.parent.mkdir(
            exist_ok=True, parents=True
        )
        with self.zipped_data_dictionary_file.open("wb") as file:
            file.write(
                requests.get(url=self.config.data_dictionary_url).content
            )

    def extract_covid_data(self) -> SourceDataHandler:
        """Extract the compressed COVID data contents.

        Also, update the internal covid_data_file attribute, so it point to the
        location of the extracted contents.

        :return: The current extractor instance.
        """
        with ZipFile(self.zipped_covid_data_file) as zip_file:
            self.temp_data_path.mkdir(parents=True, exist_ok=True)
            for file_name in zip_file.namelist():
                if "COVID19MEXICO.csv" in file_name:
                    zip_file.extract(file_name, self.temp_data_path)
                    self.covid_data_file = self.temp_data_path / file_name
                    return self
        raise ValueError(
            "The compressed file does not contain any COVID data."
        )

    def extract_dictionary_data(self) -> SourceDataHandler:
        """Extract the compresses data dictionary contents.

        Also, update the internal data_dictionary_files attribute, so it
        contains the locations of the extracted contents.

        :return: The current extractor instance.
        """
        with ZipFile(self.zipped_data_dictionary_file) as zip_file:
            self.temp_data_path.mkdir(parents=True, exist_ok=True)
            data_dictionary_files = []
            for file_name in zip_file.namelist():
                if "Catalogos" in file_name or "Descriptores" in file_name:
                    zip_file.extract(file_name, self.temp_data_path)
                    data_dictionary_files.append(
                        self.temp_data_path / file_name
                    )
        if not data_dictionary_files:
            raise ValueError(
                "The compressed file does not contain any COVID data."
            )
        self.data_dictionary_files = data_dictionary_files
        return self

    def save_covid_data_as_parquet(self):
        """Save the COVID raw data contents as a Parquet file.

        The output file will appear at the same location with the same name
        as the extracted file but with a `.parquet` extension.
        """
        destination_file = self.covid_data_file.with_suffix(".parquet")
        polars.scan_csv(
            self.covid_data_file, dtypes=SOURCE_COVID_DATA_SCHEMA
        ).sink_parquet(destination_file)
