import json
import os
from code.base_code.base_classes import AuraDBLoader, SumoApiQuery
from code.downloaders.basho_downloader import SumoApiQueryBasho
from code.downloaders.rikishi_downloader import SumoApiQueryRikishi
from code.node_builders.create_basho_nodes import AuraDBLoaderBashoNodes
from code.node_builders.create_bout_nodes import AuraDBLoaderBoutNodes
from code.node_builders.create_rikishi_nodes import AuraDBLoaderRikishiNodes
from code.relationship_builders.create_basho_bout_relationships import (
    AuraDBLoaderBashoBoutRelationships,
)
from code.relationship_builders.create_rikishi_bout_relationships import (
    AuraDBLoaderRikishiBoutRelationships,
)
from datetime import datetime
from unittest.mock import MagicMock, call, mock_open, patch

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
    @patch("code.base_code.base_classes.get_project_root")
    def test_init(
        self, mock_get_project_root, mock_datetime, mock_session, fixed_datetime
    ):
        from pathlib import Path

        mock_datetime.now.return_value = fixed_datetime
        mock_project_root = Path("/fake/project/root")
        mock_get_project_root.return_value = mock_project_root

        # Setup a MagicMock for the Session instance
        mock_session.return_value = MagicMock()

        sumo_query = SumoApiQuery()
        # Assert the properties of sumo_query
        assert sumo_query.now == fixed_datetime.strftime("%Y%m")
        assert sumo_query.log_file_name == str(
            mock_project_root / "sumo_api_query_basho.log"
        )
        assert sumo_query.iters is None
        assert (
            sumo_query.base_url
            == "https://www.sumo-api.com/api/basho/{}/banzuke/Makuuchi"
        )
        assert isinstance(sumo_query.session, MagicMock)

        # Formatting the datetime object to a string like '202301'
        formatted_date = fixed_datetime.strftime("%Y%m")
        assert sumo_query.output_dir == str(
            mock_project_root / "data" / formatted_date / "basho"
        )
        assert sumo_query.base_directory == str(mock_project_root / "data")

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
        with (
            patch("code.base_code.base_classes.logging.info"),
            patch("code.base_code.base_classes.logging.error"),
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
        assert result == "/fake/base/dir/dir3/basho", (
            "Incorrect latest directory returned"
        )

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

    @patch(
        "code.node_builders.create_rikishi_nodes.AuraDBLoaderRikishiNodes.create_rikishi_node"
    )
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=json.dumps({"id": "123", "name": "Test Rikishi"}),
    )
    @patch("os.path.join", return_value="/fakepath/fakedir/fakefile.json")
    @patch("os.listdir", return_value=["basho1.json", "basho2.json"])
    def test_load_jsons_and_create_rikishi_nodes(
        self, mock_listdir, mock_join, mock_file, mock_create_bout_node
    ):
        # Instantiate the loader here
        loader = AuraDBLoaderRikishiNodes()

        folder_path = "/fakepath/fakedir"
        loader.load_jsons_and_create_rikishi_nodes(folder_path)

        # Verify that listdir was called with the correct path
        mock_listdir.assert_called_once_with(folder_path)

        # Since we have 2 files, we expect 2 calls to create_rikishi_node.
        expected_data = {"id": "123", "name": "Test Rikishi"}
        expected_calls = [call(expected_data) for _ in range(2)]
        mock_create_bout_node.assert_has_calls(expected_calls, any_order=True)

        # Ensure the method attempts to process exactly 2 files.
        assert mock_create_bout_node.call_count == 2


class TestAuraDBLoaderBashoNodes:
    @pytest.fixture(autouse=True)
    def setup_env_vars(self, monkeypatch):
        monkeypatch.setenv("uri", "neo4j+s://test_uri")
        monkeypatch.setenv("username", "neo4j")
        monkeypatch.setenv("password", "test")

    @patch("code.base_code.base_classes.GraphDatabase.driver", return_value=MagicMock())
    def test_create_basho_node(self, mock_driver):
        # Setup the mock for the session context manager
        mock_session = MagicMock()
        mock_driver.return_value.session.return_value.__enter__.return_value = (
            mock_session
        )

        # Instantiate the class
        loader = AuraDBLoaderBashoNodes()

        # Call the method under test
        loader.create_basho_node("202001")

        # Assert session.run was called with expected arguments
        mock_session.run.assert_called_once_with(
            "MERGE (b:Basho {bashoId: $basho_id}) RETURN b", basho_id="202001"
        )

    @patch(
        "code.node_builders.create_basho_nodes.AuraDBLoaderBashoNodes.create_basho_node"
    )
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=json.dumps({"bashoId": "195803"}),
    )
    @patch("os.path.join", return_value="/fakepath/fakedir/fakefile.json")
    @patch("os.listdir", return_value=["basho1.json", "basho2.json"])
    def test_load_jsons_from_folder_and_create_basho_nodes(
        self, mock_listdir, mock_join, mock_file, mock_create_basho_node
    ):
        # Instantiate the loader here
        loader = AuraDBLoaderBashoNodes()

        folder_path = "/fakepath/fakedir"
        loader.load_jsons_from_folder_and_create_basho_nodes(folder_path)

        # Verify that listdir was called with the correct path
        mock_listdir.assert_called_once_with(folder_path)

        # Since we have 2 files, we expect 2 calls to create_rikishi_node.
        expected_data = "195803"
        expected_calls = [call(expected_data) for _ in range(2)]
        mock_create_basho_node.assert_has_calls(expected_calls, any_order=True)

        # Ensure the method attempts to process exactly 2 files.
        assert mock_create_basho_node.call_count == 2


class TestAuraDBLoaderBoutNodes:
    @pytest.fixture(autouse=True)
    def setup_env_vars(self, monkeypatch):
        monkeypatch.setenv("uri", "neo4j+s://test_uri")
        monkeypatch.setenv("username", "neo4j")
        monkeypatch.setenv("password", "test")

    @patch("code.base_code.base_classes.GraphDatabase.driver", return_value=MagicMock())
    def test_create_bout_node(self, mock_driver):
        # Setup the mock for the session context manager
        mock_session = MagicMock()
        mock_driver.return_value.session.return_value.__enter__.return_value = (
            mock_session
        )

        expected_attributes = {
            "result_rikishi1": "win",
            "RikishiID_rikishi1": 123,
            "Side_rikishi1": "east",
            "kimarite": "technique",
            "result_rikishi2": "loss",
            "Fight_Number": 12,
            "RikishiID_rikishi2": 456,
            "Side_rikishi2": "west",
            "bashoId": "195903",
        }
        # Instantiate the class
        loader = AuraDBLoaderBoutNodes()

        # Call the method under test
        loader.create_bout_node(
            result_rikishi1=expected_attributes["result_rikishi1"],
            RikishiID_rikishi1=expected_attributes["RikishiID_rikishi1"],
            Side_rikishi1=expected_attributes["Side_rikishi1"],
            kimarite=expected_attributes["kimarite"],
            result_rikishi2=expected_attributes["result_rikishi2"],
            Fight_Number=expected_attributes["Fight_Number"],
            RikishiID_rikishi2=expected_attributes["RikishiID_rikishi2"],
            Side_rikishi2=expected_attributes["Side_rikishi2"],
            bashoId=expected_attributes["bashoId"],
        )

        expected_query = """MERGE (b:Bout {result_rikishi1: $result_rikishi1,rikishiId_rikishi1: $RikishiID_rikishi1,
                                          side_rikishi1: $Side_rikishi1, fightNumber: $Fight_Number,
                                           kimarite: $kimarite, result_rikishi2: $result_rikishi2,
                                           rikishiId_rikishi2: $RikishiID_rikishi2,
                                           side_rikishi2: $Side_rikishi2,bashoId:$bashoId})
                                         RETURN b"""
        # Normalize whitespace in the expected and actual query strings
        expected_query_normalized = " ".join(expected_query.split())
        actual_query_normalized = " ".join(mock_session.run.call_args[0][0].split())

        assert actual_query_normalized == expected_query_normalized

    @patch(
        "code.node_builders.create_bout_nodes.AuraDBLoaderBoutNodes.create_bout_node"
    )
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=json.dumps(
            {
                "bashoId": "195803",
                "division": "Makuuchi",
                "east": [
                    {
                        "side": "East",
                        "rikishiID": 1404,
                        "shikonaEn": "Chiyonoyama",
                        "rankValue": 102,
                        "rank": "Yokozuna 1 East",
                        "record": [
                            {
                                "result": "win",
                                "opponentShikonaEn": "Annenyama",
                                "opponentShikonaJp": "",
                                "opponentID": 1383,
                                "kimarite": "sotogake",
                            }
                        ],
                        "wins": 1,
                        "losses": 0,
                        "absences": 0,
                    }
                ],
                "west": [
                    {
                        "side": "West",
                        "rikishiID": 1383,
                        "shikonaEn": "Annenyama",
                        "rankValue": 103,
                        "rank": "Yokozuna 1 West",
                        "record": [
                            {
                                "result": "loss",
                                "opponentShikonaEn": "Chiyonoyama",
                                "opponentShikonaJp": "",
                                "opponentID": 1404,
                                "kimarite": "sotogake",
                            }
                        ],
                        "wins": 0,
                        "losses": 1,
                        "absences": 0,
                    }
                ],
            }
        ),
    )
    @patch("os.path.join", return_value="/fakepath/fakedir/fakefile.json")
    @patch("os.listdir", return_value=["basho1.json", "basho2.json"])
    def test_load_jsons_and_create_bout_nodes(
        self, mock_listdir, mock_join, mock_file, mock_create_bout_node
    ):
        # Instantiate the loader here
        loader = AuraDBLoaderBoutNodes()

        folder_path = "/fakepath/fakedir"
        loader.load_jsons_from_folder_and_create_bout_nodes(folder_path)

        # Verify that listdir was called with the correct path
        mock_listdir.assert_called_once_with(folder_path)

        # Since we have 2 files, we expect 2 calls to create_bout_node.
        expected_calls = [
            call(
                result_rikishi1="win",
                RikishiID_rikishi1=1404,
                Side_rikishi1="East",
                kimarite="sotogake",
                result_rikishi2="loss",
                Fight_Number=1,
                RikishiID_rikishi2=1383,
                Side_rikishi2="West",
                bashoId="basho1",
            ),
            call(
                result_rikishi1="win",
                RikishiID_rikishi1=1404,
                Side_rikishi1="East",
                kimarite="sotogake",
                result_rikishi2="loss",
                Fight_Number=1,
                RikishiID_rikishi2=1383,
                Side_rikishi2="West",
                bashoId="basho2",
            ),
        ]
        mock_create_bout_node.assert_has_calls(expected_calls, any_order=True)

        # Ensure the method attempts to process exactly 2 files.
        assert mock_create_bout_node.call_count == 2


# relationship builder tests
class TestAuraDBLoaderBashoBoutRelationships:
    @pytest.fixture(autouse=True)
    def setup_env_vars(self, monkeypatch):
        monkeypatch.setenv("uri", "neo4j+s://test_uri")
        monkeypatch.setenv("username", "neo4j")
        monkeypatch.setenv("password", "test")

    @patch("code.base_code.base_classes.GraphDatabase.driver", return_value=MagicMock())
    def test_create_basho_bout_relationship(self, mock_driver):
        # Setup the mock for the session context manager
        mock_session = MagicMock()
        mock_driver.return_value.session.return_value.__enter__.return_value = (
            mock_session
        )

        expected_attributes = {"bashoId": "195903"}
        # Instantiate the class
        loader = AuraDBLoaderBashoBoutRelationships()

        # Call the method under test
        loader.create_basho_bout_relationship(bashoId=expected_attributes["bashoId"])

        expected_query = """
            MATCH (b:Basho), (b2:Bout)
            WHERE b.bashoId = $bashoId AND b2.bashoId = $bashoId
            MERGE (b)-[:BOUT_EVENT]->(b2)
            """
        # Normalize whitespace in the expected and actual query strings
        expected_query_normalized = " ".join(expected_query.split())
        actual_query_normalized = " ".join(mock_session.run.call_args[0][0].split())

        assert actual_query_normalized == expected_query_normalized

    @patch(
        "code.relationship_builders.create_basho_bout_relationships.AuraDBLoaderBashoBoutRelationships.create_basho_bout_relationship"
    )
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=json.dumps({"bashoId": "195801"}),
    )
    @patch("os.path.join", return_value="/fakepath/fakedir/fakefile.json")
    @patch("os.listdir", return_value=["basho1.json", "basho2.json"])
    def test_load_jsons_and_create_rikishi_nodes(
        self, mock_listdir, mock_join, mock_file, mock_create_basho_node
    ):
        # Instantiate the loader here
        loader = AuraDBLoaderBashoBoutRelationships()

        folder_path = "/fakepath/fakedir"
        loader.run_create_basho_bout_relationship(folder_path)

        # Verify that listdir was called with the correct path
        mock_listdir.assert_called_once_with(folder_path)

        # Since we have 2 files, we expect 2 calls to create_rikishi_node.
        expected_calls = [call(bashoId="195801"), call(bashoId="195801")]
        mock_create_basho_node.assert_has_calls(expected_calls, any_order=True)

        # Ensure the method attempts to process exactly 2 files.
        assert mock_create_basho_node.call_count == 2


class TestAuraDBLoaderRikishiBoutRelationships:
    @pytest.fixture(autouse=True)
    def setup_env_vars(self, monkeypatch):
        monkeypatch.setenv("uri", "neo4j+s://test_uri")
        monkeypatch.setenv("username", "neo4j")
        monkeypatch.setenv("password", "test")

    @patch("code.base_code.base_classes.GraphDatabase.driver", return_value=MagicMock())
    def test_create_rikishi_bout_relationship(self, mock_driver):
        # Setup the mock for the session context manager
        mock_session = MagicMock()
        mock_driver.return_value.session.return_value.__enter__.return_value = (
            mock_session
        )

        expected_attributes = {"rikishiId": "1"}
        # Instantiate the class
        loader = AuraDBLoaderRikishiBoutRelationships()

        # Call the method under test
        loader.create_rikishi_bout_relationship(
            rikishiId=expected_attributes["rikishiId"]
        )

        expected_query = """MATCH (r:Rikishi)
                        WHERE r.rikishiID = $rikishiId
                        WITH r
                        MATCH (b:Bout)
                        WHERE b.rikishiId_rikishi1 = r.rikishiID OR b.rikishiId_rikishi2 = r.rikishiID
                        MERGE (r)-[:RIKISHI_IN_BOUT_EVENT]->(b)"""
        # Normalize whitespace in the expected and actual query strings
        expected_query_normalized = " ".join(expected_query.split())
        actual_query_normalized = " ".join(mock_session.run.call_args[0][0].split())

        assert actual_query_normalized == expected_query_normalized

    @patch(
        "code.relationship_builders.create_rikishi_bout_relationships.AuraDBLoaderRikishiBoutRelationships.create_rikishi_bout_relationship"
    )
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=json.dumps({"id": "123", "name": "Test Rikishi"}),
    )
    @patch("os.path.join", return_value="/fakepath/fakedir/fakefile.json")
    @patch("os.listdir", return_value=["1.json", "2.json"])
    def test_load_jsons_and_create_rikishi_nodes(
        self, mock_listdir, mock_join, mock_file, mock_create_basho_node
    ):
        # Instantiate the loader here
        loader = AuraDBLoaderRikishiBoutRelationships()

        folder_path = "/fakepath/fakedir"
        loader.run_create_rikishi_bout_relationship(folder_path)

        # Verify that listdir was called with the correct path
        mock_listdir.assert_called_once_with(folder_path)

        # Since we have 2 files, we expect 2 calls to create_rikishi_node.
        expected_calls = [call(rikishiId="123"), call(rikishiId="123")]
        mock_create_basho_node.assert_has_calls(expected_calls, any_order=True)

        # Ensure the method attempts to process exactly 2 files.
        assert mock_create_basho_node.call_count == 2
