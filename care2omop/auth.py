from SPARQLWrapper import SPARQLWrapper, POST, BASIC, CSV
import sys

class ServerConnection:

    def __init__(self, config: dict):
        self.triplestore_url = config.get("TRIPLESTORE_URL")
        
        if not self.triplestore_url:
            raise ValueError("No endpoint defined in the configuration file. Please create a TRIPLESTORE parameter in your .yaml configuration.")
        
        self.triplestore_user = config.get("TRIPLESTORE_USERNAME")
        self.triplestore_pass = config.get("TRIPLESTORE_PASSWORD")

    def query_connection(self):
        endpoint = SPARQLWrapper(self.triplestore_url)
        endpoint.setHTTPAuth(BASIC)

        if self.triplestore_user and self.triplestore_pass:
            endpoint.setCredentials(self.triplestore_user, self.triplestore_pass)

        endpoint.setMethod(POST)
        endpoint.setReturnFormat(CSV)
        return endpoint
