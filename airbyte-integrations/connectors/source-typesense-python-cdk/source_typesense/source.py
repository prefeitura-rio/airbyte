#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#
import json
from pathlib import Path
from typing import Any, List, Mapping, Tuple

from typesense import Client
from typesense.exceptions import TypesenseClientError

import source_typesense
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .streams import TypesenseCollection


class SourceTypesense(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Connection check to validate that the user-provided config can be used to connect to Typesense.

        :param config: the user-input config object conforming to the connector's spec.yaml
        :param logger: logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect successfully, (False, error) otherwise.
        """
        try:
            protocol = config.get("protocol", "http")
            host = config["host"]
            port = config.get("port", 8108)
            api_key = config["api_key"]

            # Create Typesense client
            client = Client({
                'nodes': [{
                    'host': host,
                    'port': str(port),
                    'protocol': protocol
                }],
                'api_key': api_key,
                'connection_timeout_seconds': 30
            })

            # Test the connection by checking health
            health = client.operations.is_healthy()
            if health:
                logger.info("Successfully connected to Typesense server.")
                return True, None
            else:
                return False, "Typesense server is not healthy."

        except TypesenseClientError as e:
            return False, f"Typesense client error: {str(e)}"
        except Exception as e:
            return False, f"Unexpected error occurred: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Generate streams for each Typesense collection.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        try:
            protocol = config.get("protocol", "http")
            host = config["host"]
            port = config.get("port", 8108)
            api_key = config["api_key"]
            page_size = config.get("page_size", 250)

            # Create Typesense client
            config_dict = {
                'nodes': [{
                    'host': host,
                    'port': str(port),
                    'protocol': protocol
                }],
                'api_key': api_key,
                'connection_timeout_seconds': 30
            }

            client = Client(config_dict)

            # Get list of collections
            collections_response = client.collections.retrieve()

            # Dynamically generate streams for each collection
            schemas_path = Path(source_typesense.__file__).parent / "schemas"
            output_schemas_path = schemas_path

            # Load schema template (fixed schema with id and data fields)
            with open(schemas_path / "collection.json", "r") as f:
                collection_schema = json.load(f)

            streams = []

            for collection_info in collections_response:
                collection_name = collection_info["name"]

                # Generate schema file for the collection
                schema_file = output_schemas_path / f"{collection_name}.json"
                schema_file.parent.mkdir(parents=True, exist_ok=True)
                with open(schema_file, "w") as f:
                    json.dump(collection_schema, f, indent=2)

                # Create dynamic class for the stream
                stream_class = type(
                    f"{collection_name.title().replace('_', '').replace('-', '')}",
                    (TypesenseCollection,),
                    {
                        "name": collection_name,
                        "__module__": "source_typesense.streams"
                    }
                )

                streams.append(
                    stream_class(
                        collection_name=collection_name,
                        protocol=protocol,
                        host=host,
                        port=port,
                        api_key=api_key,
                        page_size=page_size
                    )
                )

            return streams

        except Exception as e:
            raise Exception(f"Failed to create streams: {str(e)}")
