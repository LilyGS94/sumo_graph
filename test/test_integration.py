import json

import jsonschema
import pytest
import requests  # type: ignore
from jsonschema import validate
from requests.adapters import HTTPAdapter  # type: ignore


# hey buddy
def load_schema(file_path):
    with open(file_path) as file:
        return json.load(file)


# Parametrize decorator to run the test with different URL and schema values
@pytest.mark.parametrize(
    "url, schema_file",
    [
        (
            "https://www.sumo-api.com/api/basho/202301/banzuke/Makuuchi",
            "./test_data/basho_test_response_schema.json",
        ),
        (
            "https://www.sumo-api.com/api/rikishi/1?intai=true",
            "./test_data/rikishi_test_response_schema.json",
        ),
    ],
)
def test_response_schema(url, schema_file):
    # Load the schema
    schema = load_schema(schema_file)
    # Set up the session and make the request
    # url = "https://www.sumo-api.com/api/basho/202301/banzuke/Makuuchi"
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    response = session.get(url)
    response.raise_for_status()
    response_data = response.json()

    # Validate the response
    try:
        validate(instance=response_data, schema=schema)
        print("Validation successful: Data conforms to the schema.")
    except jsonschema.exceptions.ValidationError as ve:
        print("Validation failed:", ve)
