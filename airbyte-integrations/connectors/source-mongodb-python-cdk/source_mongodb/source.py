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
    def infer_schema_from_sample(self, collection, base_schema: dict, sample_size: int = 100, fields_filter: list = None) -> dict:
        """
        Infer JSON schema from sample documents in a MongoDB collection.
        Uses MongoDB aggregation for efficient field discovery.
        
        Args:
            collection: MongoDB collection object
            base_schema: Base schema template to extend
            sample_size: Number of documents to sample for schema inference
            fields_filter: List of fields to include (None = all fields)
            
        Returns:
            dict: JSON schema with inferred properties
        """
        # Start with base schema
        schema = base_schema.copy()
        properties = schema.get("properties", {})
        
        # Use aggregation to efficiently discover fields
        pipeline = [
            {"$sample": {"size": sample_size}},
            {"$project": {"document": "$$ROOT"}},
            {"$replaceRoot": {"newRoot": "$document"}}
        ]
        
        try:
            sample_docs = list(collection.aggregate(pipeline))
        except Exception:
            # Fallback to simple find if aggregation fails
            sample_docs = list(collection.find().limit(sample_size))
        
        if not sample_docs:
            return schema
        
        # Analyze each document to infer schema
        all_fields = set()
        field_types = {}
        
        for doc in sample_docs:
            # Get all field paths (including nested fields)
            field_paths = self._extract_field_paths(doc)
            
            for field_path, value in field_paths.items():
                # Apply field filter if specified
                if fields_filter and field_path not in fields_filter:
                    continue
                    
                all_fields.add(field_path)
                
                # Infer field type
                if field_path not in field_types:
                    field_types[field_path] = set()
                
                field_type = self._infer_field_type(value)
                field_types[field_path].add(field_type)
        
        # Generate properties for each field
        for field in all_fields:
            types = list(field_types[field])
            
            if field == "_id":
                # Always treat _id as string (converted from ObjectId)
                properties[field] = {"type": "string", "description": "MongoDB document identifier"}
            else:
                # Handle multiple types
                if len(types) == 1:
                    properties[field] = {"type": types[0]}
                else:
                    # Multiple types possible - remove duplicates and null if other types exist
                    types = list(set(types))
                    if len(types) > 1 and "null" in types and len(types) > 1:
                        # Make it nullable
                        non_null_types = [t for t in types if t != "null"]
                        if len(non_null_types) == 1:
                            properties[field] = {"type": [non_null_types[0], "null"]}
                        else:
                            properties[field] = {"type": types}
                    else:
                        properties[field] = {"type": types}
        
        schema["properties"] = properties
        return schema
    
    def _extract_field_paths(self, doc, prefix=""):
        """
        Extract all field paths from a document, including nested fields.
        
        Args:
            doc: Document to analyze
            prefix: Current field path prefix
            
        Returns:
            dict: Field paths mapped to their values
        """
        field_paths = {}
        
        if not isinstance(doc, dict):
            return {prefix: doc} if prefix else {}
        
        for key, value in doc.items():
            field_path = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict) and value:
                # Nested object - extract nested fields
                nested_paths = self._extract_field_paths(value, field_path)
                field_paths.update(nested_paths)
            else:
                # Leaf value
                field_paths[field_path] = value
                
        return field_paths
    
    def _infer_field_type(self, value):
        """
        Infer JSON schema type from a value.
        
        Args:
            value: Value to analyze
            
        Returns:
            str: JSON schema type
        """
        if value is None:
            return "null"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, bool):  # Check bool before int (bool is subclass of int)
            return "boolean"
        elif isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "number"
        elif isinstance(value, list):
            return "array"
        elif isinstance(value, dict):
            return "object"
        else:
            # Handle other types (dates, ObjectId, etc.)
            return "string"
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
            
            # Dynamically generate streams for each collection (like CouchDB connector)
            schemas_path = Path(source_mongodb.__file__).parent / "schemas"
            # Create schema files in the package schemas directory for CDK to find
            output_schemas_path = schemas_path
            
            # Load schema templates
            with open(schemas_path / "collection.json", "r") as f:
                base_schema_template = json.load(f)
            
            streams = []
            
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
                    
                    # Get configuration options
                    sample_size = config.get("sample_size", 100)
                    fields_filter = config.get("fields")
                    
                    # Generate dynamic schema based on sample documents
                    inferred_schema = self.infer_schema_from_sample(
                        collection, 
                        base_schema_template, 
                        sample_size=sample_size,
                        fields_filter=fields_filter
                    )
                    
                    # Create incremental stream if _id exists or specified cursor field
                    cursor_field = config.get("cursor_field", "_id")
                    if sample_doc and cursor_field in sample_doc:
                        # Generate schema file for incremental stream
                        schema_file = output_schemas_path / f"{stream_name}.json"
                        schema_file.parent.mkdir(parents=True, exist_ok=True)
                        with open(schema_file, "w") as f:
                            json.dump(inferred_schema, f, indent=2)
                        
                        # Create dynamic class for incremental stream
                        stream_class = type(
                            f"{stream_name.title().replace('_', '')}Incremental",
                            (MongoCollectionIncremental,),
                            {
                                "name": stream_name,
                                "cursor_field": cursor_field,
                                "__module__": "source_mongodb.streams"  # Ensure correct module for schema loading
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
                        # Generate schema file for full refresh stream
                        schema_file = output_schemas_path / f"{stream_name}.json"
                        schema_file.parent.mkdir(parents=True, exist_ok=True)
                        with open(schema_file, "w") as f:
                            json.dump(inferred_schema, f, indent=2)
                        
                        # Create dynamic class for full refresh stream
                        stream_class = type(
                            f"{stream_name.title().replace('_', '')}",
                            (MongoCollection,),
                            {
                                "name": stream_name,
                                "__module__": "source_mongodb.streams"  # Ensure correct module for schema loading
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