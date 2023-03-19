"""Routines to download and extract the source data."""
from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from zipfile import ZipFile

import requests
from attr import dataclass


@dataclass
class DataExtractor:
    """Uncompress the data file contents."""

    # Path to the compressed dictionaries and catalogs.
    path: Path

    def extract(self, path: Path) -> Path:
        """Extract the compresses contents to the given path.

        :param path: The output path.
        """
        path.mkdir(parents=True, exist_ok=True)
        with ZipFile(self.path) as zip_file:
            for file_name in zip_file.namelist():
                if "COVID19MEXICO.csv" in file_name:
                    zip_file.extract(file_name, path)
                    file_path = path / file_name
                    return file_path
        raise ValueError(
            "The compressed file does not contain any COVID data."
        )


@dataclass
class DataDictionaryExtractor:
    """Uncompress the data dictionary file contents."""

    # Path to the compressed dictionaries and catalogs.
    path: Path

    def extract(self, path: Path) -> list[Path]:
        """Extract the compresses contents to the given path.

        :param path: The output path.
        """
        path.mkdir(parents=True, exist_ok=True)
        with ZipFile(self.path) as zip_file:
            file_paths = []
            for file_name in zip_file.namelist():
                if "Catalogos" in file_name or "Descriptores" in file_name:
                    zip_file.extract(file_name, path)
                    file_paths.append(path / file_name)
        if not file_paths:
            raise ValueError(
                "The compressed file does not contain any COVID data."
            )
        return file_paths


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
    download_path: Path

    @property
    def extractor(self) -> DataExtractor:
        """Extractor instance used to uncompress the data contents."""
        return DataExtractor(self.download_path)

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
            self.download_path.parent.mkdir(exist_ok=True, parents=True)
            with self.download_path.open("wb") as file:
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
    download_path: Path

    @property
    def extractor(self) -> DataDictionaryExtractor:
        """Extractor instance used to uncompress the data contents."""
        return DataDictionaryExtractor(self.download_path)

    def download(self):
        """Download the COVID data dictionaries."""
        print("Downloading dictionary data...")
        self.download_path.parent.mkdir(exist_ok=True, parents=True)
        with self.download_path.open("wb") as file:
            file.write(requests.get(url=self.data_url).content)
