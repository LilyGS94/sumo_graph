import os

from dotenv import load_dotenv
from neo4j import GraphDatabase

# Constants
data_path = "data"  # Path to the 'data' directory
# Load the .env file
load_dotenv()
# Accessing variables
password = os.environ.get("password")
uri = os.environ.get("uri")
username = os.environ.get("username")


class AuraDBLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        if self.driver:
            self.driver.close()

    def create_basho_bout_relationship(self):
        with self.driver.session() as session:
            query = (
                "MATCH (b:Basho), (b2:Bout) "
                "WHERE b.bashoId = b2.bashoId"
                "MERGE (b)-[:BOUT_EVENT]->(b2) "
                "RETURN count(*) as relationshipsCreated"
            )
            result = session.run(query)
            return result.single()[0]

    def run_create_basho_bout_relationship(self):
        self.create_basho_bout_relationship()


# Main execution
if __name__ == "__main__":
    loader = AuraDBLoader(uri, username, password)
    try:
        loader.create_basho_bout_relationship()
    finally:
        loader.close()
