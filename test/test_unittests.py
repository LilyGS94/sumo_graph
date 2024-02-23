import os
from code.base_code.base_classes import AuraDBLoader, SumoApiQuery
from code.downloaders.basho_downloader import SumoApiQueryBasho
from code.downloaders.rikishi_downloader import SumoApiQueryRikishi
from code.node_builders.create_rikishi_nodes import AuraDBLoaderRikishiNodes
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest


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
    @patch("code.base_code.base_classes.requests.Session")  # Mock the Session
    @patch("code.base_code.base_classes.datetime")
    def test_init(self, mock_datetime, mock_session, fixed_datetime):
        mock_datetime.now.return_value = fixed_datetime

        # Setup a MagicMock for the Session instance
        mock_session.return_value = MagicMock()

        sumo_query = SumoApiQuery()
        # Assert the properties of sumo_query
        assert sumo_query.now == fixed_datetime.strftime("%Y%m")
        assert sumo_query.log_file_name == "../../sumo_api_query_basho.log"
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
        monkeypatch.setattr(
            "code.base_code.base_classes.requests.Session.get", mock_get
        )
        monkeypatch.setattr("builtins.open", mock_open)
        monkeypatch.setattr("code.base_code.base_classes.json.dump", mock_json_dump)
        monkeypatch.setattr("code.base_code.base_classes.os.path.join", mock_os_join)
        with patch("code.base_code.base_classes.logging.info"), patch(
            "code.base_code.base_classes.logging.error"
        ):
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
        monkeypatch.setattr(
            "code.base_code.base_classes.logging.basicConfig", mock_logging
        )
        sumo_query = SumoApiQuery()
        sumo_query.setup_logging()
        mock_logging.assert_called_once()

    @patch(
        "code.base_code.base_classes.SumoApiQuery.query_endpoint"
    )  # Mock the query_endpoint method
    @patch("code.base_code.base_classes.logging")  # Mock the logging module
    @patch("code.base_code.base_classes.os.path.exists")
    @patch("code.base_code.base_classes.os.makedirs")
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
    @patch("code.downloaders.basho_downloader.datetime")
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
    @patch("code.downloaders.rikishi_downloader.os.path.getmtime")
    @patch("code.downloaders.rikishi_downloader.os.path.isdir")
    @patch("code.downloaders.rikishi_downloader.os.listdir")
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

    @patch("code.downloaders.rikishi_downloader.os.listdir")
    def test_get_latest_directory_empty(self, mock_listdir, base_directory_and_dirs):
        base_directory, _ = base_directory_and_dirs
        mock_listdir.return_value = []
        sumo_rikishi = SumoApiQueryRikishi()
        sumo_rikishi.base_directory = base_directory

        result = sumo_rikishi.get_latest_directory()

        # Assertion for empty directory
        mock_listdir.assert_called_once_with(base_directory)
        assert result is None, "Expected None for empty base directory"


class TestAuraDBLoader:
    @pytest.fixture(autouse=True)
    def setup_env_vars(self, monkeypatch):
        # Correctly use monkeypatch as an argument to the method
        monkeypatch.setenv("uri", "neo4j+s://test_uri")
        monkeypatch.setenv("username", "test_user")
        monkeypatch.setenv("password", "test_password")

    def test_initialization(self, mocker):
        # Mock the GraphDatabase.driver method
        mock_driver = mocker.patch("code.base_code.base_classes.GraphDatabase.driver")
        mocker.patch("code.base_code.base_classes.load_dotenv")

        loader = AuraDBLoader()
        print(loader)
        # Assertions to verify correct initialization
        mock_driver.assert_called_once_with(
            "neo4j+s://test_uri", auth=("test_user", "test_password")
        )

    def test_close(self, mocker):
        # Mock the driver instance to verify close is called on it
        mock_driver_instance = mocker.MagicMock()
        mocker.patch(
            "code.base_code.base_classes.GraphDatabase.driver",
            return_value=mock_driver_instance,
        )

        loader = AuraDBLoader()
        loader.close()

        # Verify close was called
        mock_driver_instance.close.assert_called_once()

    def test_get_most_recent_directory(self, mocker):
        # Mock os.listdir and os.path.isdir to simulate filesystem behavior
        mocker.patch(
            "code.base_code.base_classes.os.listdir",
            return_value=["202301", "202302", "202203", "not_a_date"],
        )
        mocker.patch("code.base_code.base_classes.os.path.isdir", lambda x: True)

        loader = AuraDBLoader()
        most_recent_dir = loader.get_most_recent_directory("base_path")

        # Assert the correct directory is identified
        assert most_recent_dir == "202302"


# node testers
class TestAuraDBLoaderRikishiNodes:
    @pytest.fixture(autouse=True)
    def setup_env_vars(self, monkeypatch):
        monkeypatch.setenv("uri", "neo4j+s://test_uri")
        monkeypatch.setenv("username", "neo4j")
        monkeypatch.setenv("password", "test")

    @patch("code.base_code.base_classes.GraphDatabase.driver", return_value=MagicMock())
    def test_create_rikishi_node(self, mock_driver):
        # Setup the mock for the session context manager
        mock_session = MagicMock()
        mock_driver.return_value.session.return_value.__enter__.return_value = (
            mock_session
        )

        # Instantiate the class
        loader = AuraDBLoaderRikishiNodes()

        # Test data
        rikishi_data = {"id": "12345", "other_attribute": "value"}

        # Call the method under test
        loader.create_rikishi_node(rikishi_data)

        # Expected data after method manipulates it
        expected_attributes = {
            "rikishiID": "12345",
            "name": "12345",
            "other_attribute": "value",
        }

        # Assert session.run was called with expected arguments
        mock_session.run.assert_called_once_with(
            "MERGE (r:Rikishi {rikishiID: $rikishiID}) SET r += $attributes RETURN r",
            rikishiID="12345",
            attributes=expected_attributes,
        )


# relationship testers
