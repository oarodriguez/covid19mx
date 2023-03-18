"""Test the downloader module routines."""
from pathlib import Path

import pytest
import responses

from covid19mx.config import (
    COVID_DATA_FILENAME,
    COVID_DATA_URL,
    DATA_DICTIONARY_FILENAME,
    DATA_DICTIONARY_URL,
)
from covid19mx.extraction import Downloader

ARE_WE_USING_MOCK_DATA = True

MOCK_DATA_PATH = Path(__file__).parent.parent.parent / "data"
MOCK_COVID_DATA_PATH = MOCK_DATA_PATH / COVID_DATA_FILENAME
MOCK_DATA_DICTIONARY_PATH = MOCK_DATA_PATH / DATA_DICTIONARY_FILENAME

MOCK_COVID_DATA_SIZE = MOCK_COVID_DATA_PATH.stat().st_size
MOCK_DATA_DICTIONARY_SIZE = MOCK_DATA_DICTIONARY_PATH.stat().st_size

MOCK_COVID_DATA_DOWNLOAD_PATH = (
    MOCK_DATA_PATH / "downloads" / COVID_DATA_FILENAME
)
MOCK_DATA_DICTIONARY_DOWNLOAD_PATH = (
    MOCK_DATA_PATH / "downloads" / DATA_DICTIONARY_FILENAME
)


@pytest.fixture(scope="module")
def downloader():
    """Return a new `Downloader` instance."""
    return Downloader(COVID_DATA_URL, DATA_DICTIONARY_URL)


@pytest.mark.skipif(
    condition=not ARE_WE_USING_MOCK_DATA,
    reason="This routine downloads a huge data file right now. We have to "
    "set mock data before enabling it.",
)
def test_download_covid_data(downloader: Downloader):
    """Test the routineS used to download the COVID data."""
    mock_headers = {
        "Content-Length": f"{MOCK_COVID_DATA_SIZE}",
        "Accept-Ranges": "bytes",
        "Content-Type": "application/x-zip-compressed",
    }
    downloaded_size = 0
    with responses.RequestsMock() as requests_mock:
        requests_mock.add(
            method="HEAD",
            url=COVID_DATA_URL,
            headers=mock_headers,
        )
        with MOCK_COVID_DATA_PATH.open("rb") as file:
            file_contents = file.read()
        requests_mock.add(
            method="GET",
            url=COVID_DATA_URL,
            headers=mock_headers,
            body=file_contents,
        )

        for chunk_info in downloader.download_covid_data(
            MOCK_COVID_DATA_DOWNLOAD_PATH
        ):
            downloaded_size += chunk_info.chunk_size

    assert MOCK_COVID_DATA_DOWNLOAD_PATH.stat().st_size == downloaded_size
    assert MOCK_COVID_DATA_DOWNLOAD_PATH.stat().st_size == MOCK_COVID_DATA_SIZE


@pytest.mark.skipif(
    condition=not ARE_WE_USING_MOCK_DATA,
    reason="This routine downloads a huge data file right now. We have to "
    "set mock data before enabling it.",
)
def test_download_data_dictionaries(downloader: Downloader):
    """Test the routine used to download the COVID dictionary data."""
    mock_headers = {
        "Content-Length": f"{MOCK_DATA_DICTIONARY_SIZE}",
        "Accept-Ranges": "bytes",
        "Content-Type": "application/x-zip-compressed",
    }
    with responses.RequestsMock() as requests_mock:
        with MOCK_DATA_DICTIONARY_PATH.open("rb") as file:
            file_contents = file.read()
        requests_mock.add(
            method="GET",
            url=DATA_DICTIONARY_URL,
            headers=mock_headers,
            body=file_contents,
        )
        downloader.download_data_dictionary(MOCK_DATA_DICTIONARY_DOWNLOAD_PATH)

    assert (
        MOCK_DATA_DICTIONARY_DOWNLOAD_PATH.stat().st_size
        == MOCK_DATA_DICTIONARY_SIZE
    )
