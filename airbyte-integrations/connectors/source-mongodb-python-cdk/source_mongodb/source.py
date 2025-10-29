#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#
import json
from pathlib import Path
from typing import Any, List, Mapping, Tuple

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError

import source_mongodb
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .streams import MongoCollection, MongoCollectionIncremental


class SourceMongodb(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Connection check to validate that the user-provided config can be used to connect to MongoDB.

        :param config: the user-input config object conforming to the connector's spec.yaml
        :param logger: logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect successfully, (False, error) otherwise.
        """
        try:
            connection_string = config["connection_string"]
            
            # Create MongoDB client with timeout
            client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=30000,  # 30 seconds
                connectTimeoutMS=30000,
                socketTimeoutMS=30000
            )
            
            # Test the connection
            client.admin.command('ping')
            logger.info("Successfully connected to MongoDB server.")
            client.close()
            return True, None
            
        except OperationFailure as e:
            if e.code == 18:  # Authentication failed error code
                return False, "Authentication failed. Please check your connection string credentials."
            return False, f"Operation failed: {str(e)}"
        except ServerSelectionTimeoutError:
            return False, "Unable to connect to MongoDB server. Check if the server is running and the connection string is correct."
        except ConnectionFailure as e:
            return False, f"Connection failed: {str(e)}"
        except Exception as e:
            return False, f"Unexpected error occurred: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Generate streams for each MongoDB collection.
        
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        try:
            connection_string = config["connection_string"]
            page_size = config.get("page_size", 1000)
            
            # Create MongoDB client
            client = MongoClient(connection_string)
            
            # Get list of databases
            if config.get("database"):
                # Use specific database
                databases = [config["database"]]
            else:
                # Get all databases (excluding system databases)
                databases = [db for db in client.list_database_names() 
                           if db not in ['admin', 'local', 'config']]
            
            # Dynamically generate streams for each collection
            schemas_path = Path(source_mongodb.__file__).parent / "schemas"
            output_schemas_path = schemas_path

            # Load schema templates (fixed schema with id and data fields)
            with open(schemas_path / "collection.json", "r") as f:
                full_refresh_schema = json.load(f)

            with open(schemas_path / "collection_incremental.json", "r") as f:
                incremental_schema = json.load(f)

            streams = []
            fields_filter = config.get("fields")

            for database_name in databases:
                db = client[database_name]

                # Get all collections in the database
                collection_names = db.list_collection_names()

                for collection_name in collection_names:
                    # Skip system collections
                    if collection_name.startswith('system.'):
                        continue

                    stream_name = f"{database_name}_{collection_name}"

                    # Check if collection has suitable fields for incremental sync
                    collection = db[collection_name]
                    sample_doc = collection.find_one()

                    # Create incremental stream if _id exists or specified cursor field
                    cursor_field = config.get("cursor_field", "_id")
                    if sample_doc and cursor_field in sample_doc:
                        # Use incremental schema
                        schema_file = output_schemas_path / f"{stream_name}.json"
                        schema_file.parent.mkdir(parents=True, exist_ok=True)
                        with open(schema_file, "w") as f:
                            json.dump(incremental_schema, f, indent=2)

                        # Create dynamic class for incremental stream
                        stream_class = type(
                            f"{stream_name.title().replace('_', '')}Incremental",
                            (MongoCollectionIncremental,),
                            {
                                "name": stream_name,
                                "cursor_field": cursor_field,
                                "__module__": "source_mongodb.streams"
                            }
                        )

                        streams.append(
                            stream_class(
                                database_name=database_name,
                                collection_name=collection_name,
                                connection_string=connection_string,
                                page_size=page_size,
                                cursor_field=cursor_field,
                                fields_filter=fields_filter
                            )
                        )
                    else:
                        # Use full refresh schema
                        schema_file = output_schemas_path / f"{stream_name}.json"
                        schema_file.parent.mkdir(parents=True, exist_ok=True)
                        with open(schema_file, "w") as f:
                            json.dump(full_refresh_schema, f, indent=2)

                        # Create dynamic class for full refresh stream
                        stream_class = type(
                            f"{stream_name.title().replace('_', '')}",
                            (MongoCollection,),
                            {
                                "name": stream_name,
                                "__module__": "source_mongodb.streams"
                            }
                        )

                        streams.append(
                            stream_class(
                                database_name=database_name,
                                collection_name=collection_name,
                                connection_string=connection_string,
                                page_size=page_size,
                                fields_filter=fields_filter
                            )
                        )
            
            client.close()
            return streams
            
        except Exception as e:
            raise Exception(f"Failed to create streams: {str(e)}")