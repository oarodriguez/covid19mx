"""Routines to download and extract the source data."""
from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from zipfile import ZipFile

import requests
from attr import dataclass, field

from covid19mx.config import Config


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
    covid_data_zipped_filename: str

    # The filename we assign to the downloaded compressed dictionary data file.
    data_dictionary_zipped_filename: str

    @classmethod
    def default(cls):
        """Return a new configuration instance with the default attributes."""
        config = Config.default()
        return cls(
            config.data_path,
            config.covid_data_url,
            config.data_dictionary_url,
            config.covid_data_zipped_filename,
            config.data_dictionary_zipped_filename,
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
    def covid_data_zipped_file(self):
        """Location of the downloaded COVID data compressed file."""
        return self.temp_data_path / self.config.covid_data_zipped_filename

    @property
    def data_dictionary_zipped_file(self):
        """Location of the downloaded COVID data dictionary compressed file."""
        return (
            self.temp_data_path / self.config.data_dictionary_zipped_filename
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
            self.covid_data_zipped_file.parent.mkdir(
                exist_ok=True, parents=True
            )
            with self.covid_data_zipped_file.open("wb") as file:
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
        self.data_dictionary_zipped_file.parent.mkdir(
            exist_ok=True, parents=True
        )
        with self.data_dictionary_zipped_file.open("wb") as file:
            file.write(
                requests.get(url=self.config.data_dictionary_url).content
            )

    def extract_covid_data(self) -> SourceDataHandler:
        """Extract the compressed COVID data contents.

        Also, update the internal covid_data_file attribute, so it point to the
        location of the extracted contents.

        :return: The current extractor instance.
        """
        with ZipFile(self.covid_data_zipped_file) as zip_file:
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
        with ZipFile(self.data_dictionary_zipped_file) as zip_file:
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
