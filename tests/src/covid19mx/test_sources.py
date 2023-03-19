"""Test the downloader module routines."""
from pathlib import Path

import pytest
import responses

from covid19mx.sources import SourceDataHandler, SourceDataHandlerConfig

ARE_WE_USING_MOCK_DATA = True


@pytest.fixture()
def data_handler(source_data_config: SourceDataHandlerConfig, tmp_path: Path):
    """Mock a few HTTP responses than affect a new data handler instance."""
    with responses.RequestsMock(
        assert_all_requests_are_fired=False
    ) as requests_mock:
        # Set mock responses affecting how we download the mock COVID data
        # zipped file.
        mock_data_zipped_file = (
            source_data_config.data_path
            / source_data_config.zipped_covid_data_filename
        )
        mock_headers = {
            "Content-Length": f"{mock_data_zipped_file.stat().st_size}",
            "Accept-Ranges": "bytes",
            "Content-Type": "application/x-zip-compressed",
        }
        requests_mock.add(
            method="HEAD",
            url=source_data_config.covid_data_url,
            headers=mock_headers,
        )
        with mock_data_zipped_file.open("rb") as file:
            file_contents = file.read()
        requests_mock.add(
            method="GET",
            url=source_data_config.covid_data_url,
            headers=mock_headers,
            body=file_contents,
        )

        # Set mock responses affecting how we download the mock data
        # dictionary zipped file.
        mock_data_zipped_file = (
            source_data_config.data_path
            / source_data_config.zipped_data_dictionary_filename
        )
        mock_headers = {
            "Content-Length": f"{mock_data_zipped_file.stat().st_size}",
            "Accept-Ranges": "bytes",
            "Content-Type": "application/x-zip-compressed",
        }
        with mock_data_zipped_file.open("rb") as file:
            file_contents = file.read()
        requests_mock.add(
            method="GET",
            url=source_data_config.data_dictionary_url,
            headers=mock_headers,
            body=file_contents,
        )
        yield SourceDataHandler(source_data_config, temp_data_path=tmp_path)


@pytest.mark.skipif(
    condition=not ARE_WE_USING_MOCK_DATA,
    reason="This routine downloads a huge data file right now. We have to "
    "set mock data before enabling it.",
)
def test_download_data(
    data_handler: SourceDataHandler,
    source_data_config: SourceDataHandlerConfig,
):
    """Test the routines used to download all the COVID-related data."""
    # Download the COVID data and compare the downloaded file size
    # against the mock COVID data file.
    downloaded_size = 0
    for chunk_info in data_handler.download_covid_data_chunks():
        downloaded_size += chunk_info.chunk_size
    mock_covid_data_zipped_file = (
        source_data_config.data_path
        / source_data_config.zipped_covid_data_filename
    )
    # Check that the chunks yield the correct size information.
    assert (
        data_handler.zipped_covid_data_file.stat().st_size == downloaded_size
    )
    assert (
        data_handler.zipped_covid_data_file.stat().st_size
        == mock_covid_data_zipped_file.stat().st_size
    )

    # Download the data dictionary and compare the downloaded file size
    # against the mock dictionary data file.
    data_handler.download_data_dictionary()
    mock_covid_data_dictionary_zipped_file = (
        source_data_config.data_path
        / source_data_config.zipped_data_dictionary_filename
    )
    assert (
        data_handler.zipped_data_dictionary_file.stat().st_size
        == mock_covid_data_dictionary_zipped_file.stat().st_size
    )


def test_data_extraction(
    data_handler: SourceDataHandler,
    source_data_config: SourceDataHandlerConfig,
):
    """Test the routines used to extract all the COVID-related data."""
    for _ in data_handler.download_covid_data_chunks():
        continue
    data_handler.extract_covid_data()

    data_handler.download_data_dictionary()
    data_handler.extract_dictionary_data()
