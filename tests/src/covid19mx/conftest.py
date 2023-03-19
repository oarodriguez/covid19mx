"""Define common fixtures to be shared across tests."""
from pathlib import Path

import pytest
from attr import evolve

from covid19mx.sources import SourceDataHandlerConfig

# Location of our mock data files.
MOCK_DATA_PATH = Path(__file__).parent.parent.parent / "data"


@pytest.fixture()
def source_data_config():
    """Create a config object referring to our mock data path."""
    default_config = SourceDataHandlerConfig.default()
    yield evolve(default_config, data_path=MOCK_DATA_PATH)
