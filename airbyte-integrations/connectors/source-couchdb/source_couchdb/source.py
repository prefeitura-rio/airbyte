#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from requests.exceptions import ConnectionError, Timeout, HTTPError

from .streams import Customers, Employees

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""

class SourceCouchdb(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Connection check to validate that the user-provided config can be used to connect to the underlying API and access the specified database.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            host = config["host"]
            port = config["port"]
            username = config["username"]
            password = config["password"]
            database = config["database"]
        except KeyError as e:
            return False, f"KeyError: {str(e)} is required."

        try:
            timeout = 60  # seconds
            auth = (username, password) if username and password else None
            base_url = f"http://{host}:{port}"

            # Check connection to the CouchDB server
            response = requests.get(base_url, auth=auth, timeout=timeout)
            response.raise_for_status()

            if response.status_code == 200 and "couchdb" in response.json():
                logger.info("Successfully connected to CouchDB server.")
            else:
                return False, "Unexpected response from the CouchDB server."

            # Check access to the specified database
            db_url = f"{base_url}/{database}"
            db_response = requests.get(db_url, auth=auth, timeout=timeout)

            if db_response.status_code == 200:
                logger.info(f"Successfully accessed the database: {database}.")
                return True, None
            elif db_response.status_code == 404:
                return False, f"Error: Database '{database}' not found."
            elif db_response.status_code == 403:
                return False, f"Error: Access to database '{database}' is forbidden. Check your credentials."
            else:
                return False, f"Unexpected response when accessing database '{database}': {db_response.status_code} {db_response.reason}"

        except ConnectionError:
            return False, "Error: Unable to connect to the CouchDB server. Check if the server is running and the URL is correct."
        except Timeout:
            return False, f"Error: Connection to CouchDB server timed out after {timeout} seconds."
        except HTTPError as e:
            return False, f"HTTP Error {e.response.status_code}: {e.response.reason}"
        except Exception as e:
            return False, f"Unexpected error occurred: {str(e)}"
        
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        auth = TokenAuthenticator(token="api_key")  # Oauth2Authenticator is also available if you need oauth support
        return [Customers(authenticator=auth), Employees(authenticator=auth)]
