"""Define common fixtures to be shared across tests."""
from pathlib import Path

import pytest
from _pytest.tmpdir import TempPathFactory
from attr import dataclass

from covid19mx.config import (
    COVID_DATA_FILENAME,
    COVID_DATA_URL,
    DATA_DICTIONARY_FILENAME,
    DATA_DICTIONARY_URL,
)

MOCK_DATA_PATH = Path(__file__).parent.parent.parent / "data"


@dataclass
class DataConfig:
    """Represent the configuration of a data source."""

    data_url: str
    data_filename: str
    base_path: Path
    temp_base_path: Path

    @property
    def data_path(self):
        """Path to a mock data source location."""
        return self.base_path / self.data_filename

    @property
    def temp_data_path(self):
        """Path to a temporary data source location."""
        return self.temp_base_path / self.data_filename


@pytest.fixture(scope="session")
def data_config(tmp_path_factory: TempPathFactory) -> DataConfig:
    """Return the config of the COVID mock data."""
    return DataConfig(
        COVID_DATA_URL,
        COVID_DATA_FILENAME,
        MOCK_DATA_PATH,
        tmp_path_factory.mktemp("extraction"),
    )


@pytest.fixture(scope="session")
def data_dictionary_config(tmp_path_factory: TempPathFactory) -> DataConfig:
    """Return the config of the COVID data dictionary mock data."""
    return DataConfig(
        DATA_DICTIONARY_URL,
        DATA_DICTIONARY_FILENAME,
        MOCK_DATA_PATH,
        tmp_path_factory.mktemp("extraction"),
    )
