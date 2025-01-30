#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#
import json
from pathlib import Path
from typing import Any, List, Mapping, Tuple

import requests
from requests.exceptions import ConnectionError, HTTPError, SSLError, Timeout

import source_couchdb
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import BasicHttpAuthenticator

from .streams import DocumentsIncremental


class SourceCouchdb(AbstractSource):
    def get_base_url(self, tls: bool, host: str, port: int) -> str:
        return f"https://{host}:{port}" if tls else f"http://{host}:{port}"

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Connection check to validate that the user-provided config can be used to connect to the underlying API.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            host = config["host"]
            port = config["port"]
            username = config["username"]
            password = config["password"]
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
                return True, None
            else:
                return False, "Unexpected response from the CouchDB server."
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
            page_size = config.get("pageSize", 1000)
            tls = config.get("tls", False)
            trust_certificate = config.get("trustCertificate", False)
        except KeyError as e:
            raise KeyError(f"KeyError: {str(e)} is required.")

        authenticator = BasicHttpAuthenticator(username=username, password=password)
        base_url = self.get_base_url(tls=tls, host=host, port=port)

        # List existing databases
        response = requests.get(f"{base_url}/_all_dbs", auth=authenticator)
        response.raise_for_status()
        databases = response.json()
        databases = [db for db in databases if not db.startswith("_")]

        # Dinamically generate streams for each database
        schemas_path = Path(source_couchdb.__file__).parent / "schemas"
        output_schemas_path = Path("/usr/local/lib/python3.9/schemas")  # TODO: change to a dynamic path
        with open(schemas_path / "documents_incremental.json", "r") as f:
            schema_incremental = json.load(f)
        streams = []
        for database in databases:
            url_base = f"{base_url}/{database}/"
            class_name_base = f"{database.capitalize()}"

            # Generate schema for the incremental stream
            fname = output_schemas_path / f"{database}.json"
            fname.parent.mkdir(parents=True, exist_ok=True)
            with open(fname, "w") as f:
                json.dump(schema_incremental, f, indent=2)

            # Generate incremental stream
            subclass_incremental = type(class_name_base, (DocumentsIncremental,), {"_url_base": url_base})
            streams.append(
                subclass_incremental(
                    url_base=url_base,
                    page_size=page_size,
                    trust_certificate=trust_certificate,
                    authenticator=authenticator,
                )
            )

        return streams
