import json

import jsonschema
import requests  # type: ignore
from jsonschema import validate
from requests.adapters import HTTPAdapter  # type: ignore

# The JSON schema you provided
# schema = {
#   "type": "object",
#   "properties": {
#     "bashoId": {
#       "type": "string"
#     },
#     "division": {
#       "type": "string"
#     },
#     "east": {
#       "type": "array",
#       "items": {
#         "type": "object",
#         "properties": {
#           "side": {
#             "type": "string"
#           },
#           "rikishiID": {
#             "type": "integer"
#           },
#           "shikonaEn": {
#             "type": "string"
#           },
#           "rankValue": {
#             "type": "integer"
#           },
#           "rank": {
#             "type": "string"
#           },
#           "record": {
#             "type": "array",
#             "items": {
#               "type": "object",
#               "properties": {
#                 "result": {
#                   "type": "string"
#                 },
#                 "opponentShikonaEn": {
#                   "type": "string"
#                 },
#                 "opponentShikonaJp": {
#                   "type": "string"
#                 },
#                 "opponentID": {
#                   "type": "integer"
#                 },
#                 "kimarite": {
#                   "type": "string"
#                 }
#               },
#               "required": ["result", "opponentShikonaEn", "opponentShikonaJp", "opponentID", "kimarite"]
#             }
#           },
#           "wins": {
#             "type": "integer"
#           },
#           "losses": {
#             "type": "integer"
#           },
#           "absences": {
#             "type": "integer"
#           }
#         },
#         "required": ["side", "rikishiID", "shikonaEn", "rankValue", "rank", "record", "wins", "losses", "absences"]
#       }
#     },
#     "west": {
#       "type": "array",
#       "items": {
#         "type": "object",
#         "properties": {
#           "side": {
#             "type": "string"
#           },
#           "rikishiID": {
#             "type": "integer"
#           },
#           "shikonaEn": {
#             "type": "string"
#           },
#           "rankValue": {
#             "type": "integer"
#           },
#           "rank": {
#             "type": "string"
#           },
#           "record": {
#             "type": "array",
#             "items": {
#               "type": "object",
#               "properties": {
#                 "result": {
#                   "type": "string"
#                 },
#                 "opponentShikonaEn": {
#                   "type": "string"
#                 },
#                 "opponentShikonaJp": {
#                   "type": "string"
#                 },
#                 "opponentID": {
#                   "type": "integer"
#                 },
#                 "kimarite": {
#                   "type": "string"
#                 }
#               },
#               "required": ["result", "opponentShikonaEn", "opponentShikonaJp", "opponentID", "kimarite"]
#             }
#           },
#           "wins": {
#             "type": "integer"
#           },
#           "losses": {
#             "type": "integer"
#           },
#           "absences": {
#             "type": "integer"
#           }
#         },
#         "required": ["side", "rikishiID", "shikonaEn", "rankValue", "rank", "record", "wins", "losses", "absences"]
#       }
#     }
#   },
#   "required": ["bashoId", "division", "east", "west"]
# }


def load_schema(file_path):
    with open(file_path) as file:
        return json.load(file)


def test_response_schema():
    # Load the schema
    schema = load_schema("./test_data/basho_test_response_schema.json")
    # Set up the session and make the request
    url = "https://www.sumo-api.com/api/basho/202301/banzuke/Makuuchi"
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
