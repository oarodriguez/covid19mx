"""Test the downloader module routines."""
import pytest
import responses
from tests.src.covid19mx.conftest import DataConfig

from covid19mx.extraction import DataDictionaryDownloader, DataDownloader

ARE_WE_USING_MOCK_DATA = True


@pytest.fixture(scope="module")
def data_downloader(data_config: DataConfig):
    """Return a new `DataDownloader` instance."""
    return DataDownloader(data_config.data_url)


@pytest.fixture(scope="module")
def dictionary_data_downloader(data_dictionary_config: DataConfig):
    """Return a new `DictionaryDataDownloader` instance."""
    return DataDictionaryDownloader(data_dictionary_config.data_url)


@pytest.mark.skipif(
    condition=not ARE_WE_USING_MOCK_DATA,
    reason="This routine downloads a huge data file right now. We have to "
    "set mock data before enabling it.",
)
def test_download_covid_data(
    data_downloader: DataDownloader, data_config: DataConfig
):
    """Test the routineS used to download the COVID data."""
    data_size = data_config.data_path.stat().st_size
    mock_headers = {
        "Content-Length": f"{data_size}",
        "Accept-Ranges": "bytes",
        "Content-Type": "application/x-zip-compressed",
    }
    downloaded_size = 0
    with responses.RequestsMock() as requests_mock:
        requests_mock.add(
            method="HEAD",
            url=data_downloader.data_url,
            headers=mock_headers,
        )
        with data_config.data_path.open("rb") as file:
            file_contents = file.read()
        requests_mock.add(
            method="GET",
            url=data_downloader.data_url,
            headers=mock_headers,
            body=file_contents,
        )

        for chunk_info in data_downloader.download(data_config.temp_data_path):
            downloaded_size += chunk_info.chunk_size

    assert data_config.temp_data_path.stat().st_size == downloaded_size
    assert data_config.temp_data_path.stat().st_size == data_size


@pytest.mark.skipif(
    condition=not ARE_WE_USING_MOCK_DATA,
    reason="This routine downloads a huge data file right now. We have to "
    "set mock data before enabling it.",
)
def test_download_data_dictionaries(
    dictionary_data_downloader: DataDictionaryDownloader,
    data_dictionary_config: DataConfig,
):
    """Test the routine used to download the COVID dictionary data."""
    data_size = data_dictionary_config.data_path.stat().st_size
    mock_headers = {
        "Content-Length": f"{data_size}",
        "Accept-Ranges": "bytes",
        "Content-Type": "application/x-zip-compressed",
    }
    with responses.RequestsMock() as requests_mock:
        with data_dictionary_config.data_path.open("rb") as file:
            file_contents = file.read()
        requests_mock.add(
            method="GET",
            url=data_dictionary_config.data_url,
            headers=mock_headers,
            body=file_contents,
        )
        dictionary_data_downloader.download(
            data_dictionary_config.temp_data_path
        )

    assert data_dictionary_config.temp_data_path.stat().st_size == data_size
