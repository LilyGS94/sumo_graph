from unittest.mock import MagicMock, patch

import pytest

from base_classes import SumoApiQuery


# Fixture for creating a SumoApiQuery instance
@pytest.fixture
def sumo_query():
    instance = SumoApiQuery()
    instance.iters = ["202301"]
    return instance


def test_init(monkeypatch):
    # Mock the session and test __init__ method
    mock_session = MagicMock()
    monkeypatch.setattr("base_classes.requests.Session", mock_session)
    query = SumoApiQuery(iters=[10], pool_size=30)
    mock_session.assert_called_with()
    assert query.iters == [10]
    assert query.base_url == "https://www.sumo-api.com/api/basho/{}/banzuke/Makuuchi"


def test_query_endpoint(monkeypatch, sumo_query):
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
        sumo_query.query_endpoint("202301")
    expected_url = sumo_query.base_url.format("202301")  # Construct the expected URL
    mock_get.assert_called_once_with(
        expected_url
    )  # Check that mock_get was called with the correct URL
    mock_get.assert_called_once()
    mock_open.assert_called_once()
    mock_json_dump.assert_called_once()
    mock_os_join.assert_called_once()


def test_setup_logging(monkeypatch, sumo_query):
    # Test setup_logging method
    mock_logging = MagicMock()
    monkeypatch.setattr("base_classes.logging.basicConfig", mock_logging)
    sumo_query.setup_logging()
    mock_logging.assert_called_once()


@patch("base_classes.SumoApiQuery.query_endpoint")  # Mock the query_endpoint method
@patch("base_classes.logging")  # Mock the logging module
def test_run_queries(mock_logging, mock_query_endpoint):
    # Setup
    test_iters = ["202301", "202303", "202305"]  # Example iteration values
    sumo_api_query = SumoApiQuery(iters=test_iters)
    # Execute
    sumo_api_query.run_queries()
    # Verify
    assert mock_query_endpoint.call_count == len(test_iters)
    for iter_val in test_iters:
        mock_query_endpoint.assert_any_call(iter_val)
    mock_logging.basicConfig.assert_called_once()
