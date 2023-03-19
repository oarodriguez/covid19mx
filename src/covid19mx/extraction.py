"""Routines to download and extract the source data."""
from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from zipfile import ZipFile

import requests
from attr import dataclass, field


@dataclass
class DataExtractor:
    """Uncompress the data file contents."""

    # Path to the compressed dictionaries and catalogs.
    source_file: Path

    # The output directory path of the uncompressed files.
    destination_path: Path

    # The extracted file path.
    file_path: Path = field(default=None, init=False)

    def extract(self) -> DataExtractor:
        """Extract the compresses contents.

        Also, update the internal file_path attribute, so it point to the
        location of the extracted contents.

        :return: The current extractor instance.
        """
        self.destination_path.mkdir(parents=True, exist_ok=True)
        with ZipFile(self.source_file) as zip_file:
            for file_name in zip_file.namelist():
                if "COVID19MEXICO.csv" in file_name:
                    zip_file.extract(file_name, self.destination_path)
                    file_path = self.destination_path / file_name
                    self.file_path = file_path
                    return self
        raise ValueError(
            "The compressed file does not contain any COVID data."
        )


@dataclass
class DataDictionaryExtractor:
    """Uncompress the data dictionary file contents."""

    # Path to the compressed dictionaries and catalogs.
    source_file: Path

    # The output directory path of the uncompressed files.
    destination_path: Path

    # The extracted file path.
    file_paths: list[Path] = field(default=None, init=False)

    def extract(self) -> DataDictionaryExtractor:
        """Extract the compresses contents.

        Also, update the internal file_paths attribute, so it contains the
        locations of the extracted contents.

        :return: The current extractor instance.
        """
        self.destination_path.mkdir(parents=True, exist_ok=True)
        with ZipFile(self.source_file) as zip_file:
            file_paths = []
            for file_name in zip_file.namelist():
                if "Catalogos" in file_name or "Descriptores" in file_name:
                    zip_file.extract(file_name, self.destination_path)
                    file_paths.append(self.destination_path / file_name)
        if not file_paths:
            raise ValueError(
                "The compressed file does not contain any COVID data."
            )
        self.file_paths = file_paths
        return self


@dataclass
class DataChunkInfo:
    """Represent information about a binary data chunk."""

    # Chunk size in bytes.
    chunk_size: int

    # Total size in bytes of the object we split in several chunks.
    file_size: int


@dataclass
class DataDownloader:
    """Download the compressed COVID data and extract it."""

    # Remote URL where the data is located.
    data_url: str

    # Path we use to save the downloaded data in the filesystem.
    destination_file: Path

    def extract(self, destination_path: Path) -> DataExtractor:
        """Extractor instance used to uncompress the data contents."""
        return DataExtractor(self.destination_file, destination_path).extract()

    def download(
        self, chunk_size: int = 1024 * 1024
    ) -> Iterable[DataChunkInfo]:
        """Download the COVID data in parts.

        :param chunk_size: The size in bytes of each data part.
        """
        response = requests.head(url=self.data_url)
        response.raise_for_status()
        content_size = int(response.headers.get("Content-Length", "0"))
        with requests.get(url=self.data_url, stream=True) as response:
            response.raise_for_status()
            self.destination_file.parent.mkdir(exist_ok=True, parents=True)
            with self.destination_file.open("wb") as file:
                chunk_content: bytes
                for chunk_index, chunk_content in enumerate(
                    response.iter_content(chunk_size)
                ):
                    file.write(chunk_content)
                    yield DataChunkInfo(
                        chunk_size=len(chunk_content),
                        file_size=content_size,
                    )


@dataclass
class DataDictionaryDownloader:
    """Download the compressed COVID dictionary data and extract it."""

    # Remote URL where the data is located.
    data_url: str

    # Path we use to save the downloaded data in the filesystem.
    destination_file: Path

    def extract(self, destination_path: Path) -> DataDictionaryExtractor:
        """Extractor instance used to uncompress the data contents."""
        return DataDictionaryExtractor(
            self.destination_file, destination_path
        ).extract()

    def download(self):
        """Download the COVID data dictionaries."""
        print("Downloading dictionary data...")
        self.destination_file.parent.mkdir(exist_ok=True, parents=True)
        with self.destination_file.open("wb") as file:
            file.write(requests.get(url=self.data_url).content)
