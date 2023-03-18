"""Routines to download and extract the source data."""
from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import requests
from attr import dataclass


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

    def download(
        self, path: Path, chunk_size: int = 1024 * 1024
    ) -> Iterable[DataChunkInfo]:
        """Download the COVID data in parts.

        :param path: Path we use to save the downloaded data in the filesystem.
        :param chunk_size: The size in bytes of each data part.
        """
        response = requests.head(url=self.data_url)
        response.raise_for_status()
        content_size = int(response.headers.get("Content-Length", "0"))
        with requests.get(url=self.data_url, stream=True) as response:
            response.raise_for_status()
            path.parent.mkdir(exist_ok=True, parents=True)
            with path.open("wb") as file:
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

    def download(self, path: Path):
        """Download the COVID data dictionaries.

        :param path: Path we use to save the dictionary data in the filesystem.
        """
        print("Downloading dictionary data...")
        path.parent.mkdir(exist_ok=True, parents=True)
        with path.open("wb") as file:
            file.write(requests.get(url=self.data_url).content)
