#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#
from typing import Any, List, Mapping, Tuple

import requests
from requests.exceptions import ConnectionError, HTTPError, SSLError, Timeout

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import BasicHttpAuthenticator

from .streams import Documents, DocumentsIncremental


class SourceCouchdb(AbstractSource):
    def get_base_url(self, tls: bool, host: str, port: int) -> str:
        return f"https://{host}:{port}" if tls else f"http://{host}:{port}"

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
            tls = config.get("tls", False)
            trust_certificate = config.get("trustCertificate", False)

        except KeyError as e:
            return False, f"KeyError: {str(e)} is required."

        try:
            timeout = 60  # seconds
            auth = (username, password) if username and password else None
            base_url = self.get_base_url(tls=tls, host=host, port=port)

            # Check connection to the CouchDB server
            response = requests.get(base_url, auth=auth, timeout=timeout, verify=not trust_certificate)
            response.raise_for_status()

            if response.status_code == 200 and "couchdb" in response.json():
                logger.info("Successfully connected to CouchDB server.")
            else:
                return False, "Unexpected response from the CouchDB server."

            # Check access to the specified database
            db_url = f"{base_url}/{database}"
            db_response = requests.get(db_url, auth=auth, timeout=timeout, verify=not trust_certificate)

            if db_response.status_code == 200:
                logger.info(f"Successfully accessed the database: {database}.")
                return True, None
            elif db_response.status_code == 404:
                return False, f"Error: Database '{database}' not found."
            elif db_response.status_code == 403:
                return (
                    False,
                    f"Error: Access to database '{database}' is forbidden. Check your credentials.",
                )
            else:
                return (
                    False,
                    f"Unexpected response when accessing database '{database}': {db_response.status_code} {db_response.reason}",
                )

        except SSLError:
            return (
                False,
                "Error: SSL certificate verification failed. Check if the server's certificate is valid or disable SSL certificate verification.",
            )
        except ConnectionError:
            return (
                False,
                "Error: Unable to connect to the CouchDB server. Check if the server is running and the URL is correct.",
            )
        except Timeout:
            return (
                False,
                f"Error: Connection to CouchDB server timed out after {timeout} seconds.",
            )
        except HTTPError as e:
            return False, f"HTTP Error {e.response.status_code}: {e.response.reason}"
        except Exception as e:
            return False, f"Unexpected error occurred: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        try:
            host = config["host"]
            port = config["port"]
            username = config["username"]
            password = config["password"]
            database = config["database"]
            page_size = config.get("pageSize", 1000)
            tls = config.get("tls", False)
            trust_certificate = config.get("trustCertificate", False)
        except KeyError as e:
            raise KeyError(f"KeyError: {str(e)} is required.")

        authenticator = BasicHttpAuthenticator(username=username, password=password)
        base_url = self.get_base_url(tls=tls, host=host, port=port)
        url_base = f"{base_url}/{database}/"

        return [
            Documents(
                url_base=url_base,
                page_size=page_size,
                trust_certificate=trust_certificate,
                authenticator=authenticator,
            ),
            DocumentsIncremental(
                url_base=url_base,
                page_size=page_size,
                trust_certificate=trust_certificate,
                authenticator=authenticator,
            ),
        ]
