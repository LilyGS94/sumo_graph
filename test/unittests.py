import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from base_classes import SumoApiQuery
from get_rikishi_information import SumoApiQueryRikishi
from pulling_data import SumoApiQueryBasho


@pytest.fixture
def base_directory_and_dirs():
    base_directory = "/fake/base/dir"
    directories = ["dir1", "dir2", "dir3"]
    return base_directory, directories


@pytest.fixture
def fixed_datetime():
    # Define a fixed date for testing
    fixed_datetime = datetime(2023, 1, 1)
    return fixed_datetime


class TestSumoApiQuery:
    @patch("base_classes.requests.Session")  # Mock the Session
    @patch("base_classes.datetime")
    def test_init(self, mock_datetime, mock_session, fixed_datetime):
        mock_datetime.now.return_value = fixed_datetime

        # Setup a MagicMock for the Session instance
        mock_session.return_value = MagicMock()

        sumo_query = SumoApiQuery()
        # Assert the properties of sumo_query
        assert sumo_query.now == fixed_datetime.strftime("%Y%m")
        assert sumo_query.log_file_name == "sumo_api_query_basho.log"
        assert sumo_query.iters is None
        assert (
            sumo_query.base_url
            == "https://www.sumo-api.com/api/basho/{}/banzuke/Makuuchi"
        )
        assert isinstance(sumo_query.session, MagicMock)

        # Formatting the datetime object to a string like '202301'
        formatted_date = fixed_datetime.strftime("%Y%m")
        assert sumo_query.output_dir == f"data/{formatted_date}/basho"
        assert sumo_query.base_directory == "data/"

    def test_query_endpoint(self, monkeypatch):
        # Test query_endpoint method
        mock_get = MagicMock()
        mock_open = MagicMock()
        mock_json_dump = MagicMock()
        mock_os_join = MagicMock()
        monkeypatch.setattr("base_classes.requests.Session.get", mock_get)
        monkeypatch.setattr("builtins.open", mock_open)
        monkeypatch.setattr("base_classes.json.dump", mock_json_dump)
        monkeypatch.setattr("base_classes.os.path.join", mock_os_join)
        with patch("base_classes.logging.info"), patch("base_classes.logging.error"):
            sumo_query = SumoApiQuery()
            sumo_query.query_endpoint("202301")
        expected_url = sumo_query.base_url.format(
            "202301"
        )  # Construct the expected URL
        mock_get.assert_called_once_with(
            expected_url
        )  # Check that mock_get was called with the correct URL
        mock_get.assert_called_once()
        mock_open.assert_called_once()
        mock_json_dump.assert_called_once()
        mock_os_join.assert_called_once()

    def test_setup_logging(self, monkeypatch):
        # Test setup_logging method
        mock_logging = MagicMock()
        monkeypatch.setattr("base_classes.logging.basicConfig", mock_logging)
        sumo_query = SumoApiQuery()
        sumo_query.setup_logging()
        mock_logging.assert_called_once()

    @patch("base_classes.SumoApiQuery.query_endpoint")  # Mock the query_endpoint method
    @patch("base_classes.logging")  # Mock the logging module
    @patch("base_classes.os.path.exists")
    @patch("base_classes.os.makedirs")
    def test_run_queries(
        self, mock_makedirs, mock_path_exists, mock_logging, mock_query_endpoint
    ):
        # Setup
        test_iters = ["202301", "202303", "202305"]  # Example iteration values
        output_dir = "temp/dir"
        mock_path_exists.return_value = False
        sumo_api_query = SumoApiQuery(iters=test_iters)
        sumo_api_query.output_dir = output_dir
        # Execute
        sumo_api_query.run_queries()
        # Verify
        mock_path_exists.assert_called_once_with(output_dir)
        mock_makedirs.assert_called_once_with(output_dir)
        assert mock_query_endpoint.call_count == len(test_iters)
        for iter_val in test_iters:
            mock_query_endpoint.assert_any_call(iter_val)
        mock_logging.basicConfig.assert_called_once()


class TestSumoApiQueryBasho:
    @patch("pulling_data.datetime")
    def test_generate_timestamps(self, mock_datetime, fixed_datetime):
        # Mock the datetime to return a fixed date
        mock_datetime.now.return_value = fixed_datetime  # Example date: April 1, 2023

        # Instance of the class containing generate_timestamps
        instance = SumoApiQueryBasho()

        # Expected output
        expected_timestamps = []
        for year in range(
            1958, 2024
        ):  # 2024 because it's the year after the mocked current year
            for month in ["01", "03", "05", "07", "09", "11"]:
                if (
                    year == 2023 and int(month) > 1
                ):  # Using the mocked current year and month
                    break
                expected_timestamps.append(f"{year}{month}")

        # Call the function
        instance.generate_timestamps()

        # Assert that the generated timestamps match the expected output
        assert instance.iters == expected_timestamps


class TestSumoApiQueryRikishi:
    @patch("get_rikishi_information.os.path.getmtime")
    @patch("get_rikishi_information.os.path.isdir")
    @patch("get_rikishi_information.os.listdir")
    def test_get_latest_directory(
        self, mock_listdir, mock_isdir, mock_getmtime, base_directory_and_dirs
    ):
        base_directory, directories = base_directory_and_dirs
        full_directories = [os.path.join(base_directory, d) for d in directories]

        # Setting up mock return values
        mock_listdir.return_value = directories
        mock_isdir.side_effect = lambda d: d in full_directories
        mock_getmtime.side_effect = lambda d: {
            "/fake/base/dir/dir1": 100,
            "/fake/base/dir/dir2": 200,
            "/fake/base/dir/dir3": 300,
        }.get(d, 0)

        sumo_rikishi = SumoApiQueryRikishi()
        sumo_rikishi.base_directory = base_directory
        result = sumo_rikishi.get_latest_directory()

        # Assertions
        mock_listdir.assert_called_once_with(base_directory)
        assert all(
            mock_isdir.call_args[0][0] in full_directories
            for call in mock_isdir.call_args_list
        )
        assert (
            result == "/fake/base/dir/dir3/basho"
        ), "Incorrect latest directory returned"

    @patch("get_rikishi_information.os.listdir")
    def test_get_latest_directory_empty(self, mock_listdir, base_directory_and_dirs):
        base_directory, _ = base_directory_and_dirs
        mock_listdir.return_value = []
        sumo_rikishi = SumoApiQueryRikishi()
        sumo_rikishi.base_directory = base_directory

        result = sumo_rikishi.get_latest_directory()

        # Assertion for empty directory
        mock_listdir.assert_called_once_with(base_directory)
        assert result is None, "Expected None for empty base directory"
