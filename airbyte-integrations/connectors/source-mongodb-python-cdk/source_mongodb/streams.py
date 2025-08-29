# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from abc import ABC
from typing import Any, Iterable, Mapping, MutableMapping, Optional

from bson import ObjectId
from pymongo import MongoClient

from airbyte_cdk.sources.streams import Stream


class MongoStream(Stream, ABC):
    """
    A base class for MongoDB streams. This class provides common functionality for interacting with MongoDB collections.
    """

    def __init__(self, database_name: str, collection_name: str, connection_string: str, page_size: int, fields_filter: list = None, **kwargs):
        """
        Initialize the MongoStream.

        Args:
            database_name (str): The name of the MongoDB database.
            collection_name (str): The name of the MongoDB collection.
            connection_string (str): The MongoDB connection string.
            page_size (int): The number of records to fetch per page.
            fields_filter (list): List of fields to extract (None = all fields).
        """
        super().__init__(**kwargs)
        self.database_name = database_name
        self.collection_name = collection_name
        self.connection_string = connection_string
        self.page_size = page_size
        self.fields_filter = fields_filter
        self._client = None

    @property
    def name(self) -> str:
        """
        Get the name of the stream.

        Returns:
            str: The stream name in format "database_collection".
        """
        return f"{self.database_name}_{self.collection_name}"

    @property
    def primary_key(self) -> Optional[str]:
        """
        Get the primary key for the stream.

        Returns:
            str: The primary key field name.
        """
        return "_id"


    def get_client(self) -> MongoClient:
        """
        Get or create a MongoDB client.

        Returns:
            MongoClient: The MongoDB client instance.
        """
        if self._client is None:
            self._client = MongoClient(self.connection_string)
        return self._client

    def get_collection(self):
        """
        Get the MongoDB collection.

        Returns:
            Collection: The MongoDB collection instance.
        """
        client = self.get_client()
        db = client[self.database_name]
        return db[self.collection_name]

    def serialize_document(self, document: dict) -> dict:
        """
        Serialize MongoDB document for Airbyte.

        Args:
            document (dict): The MongoDB document.

        Returns:
            dict: The serialized document.
        """
        # Apply field filter if specified
        if self.fields_filter:
            document = {k: v for k, v in document.items() if k in self.fields_filter}
        
        # Convert ObjectId to string
        if isinstance(document.get("_id"), ObjectId):
            document["_id"] = str(document["_id"])
        
        # Handle other ObjectId fields
        for key, value in document.items():
            if isinstance(value, ObjectId):
                document[key] = str(value)
            elif isinstance(value, dict):
                document[key] = self.serialize_document(value)
            elif isinstance(value, list):
                document[key] = [self.serialize_document(item) if isinstance(item, dict) else 
                               str(item) if isinstance(item, ObjectId) else item for item in value]
        
        return document

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None) -> Iterable[Mapping[str, Any]]:
        """
        Read records from the MongoDB collection.

        Args:
            sync_mode: The sync mode (full_refresh or incremental).
            cursor_field: The cursor field for incremental syncs.
            stream_slice: The stream slice.
            stream_state: The current stream state.

        Returns:
            Iterable[Mapping[str, Any]]: An iterable of records.
        """
        collection = self.get_collection()
        
        # Build query based on sync mode
        query = {}
        if sync_mode == "incremental" and stream_state and cursor_field:
            cursor_value = stream_state.get(cursor_field)
            if cursor_value:
                if cursor_field == "_id":
                    query[cursor_field] = {"$gt": ObjectId(cursor_value)}
                else:
                    query[cursor_field] = {"$gt": cursor_value}
        
        # Sort by cursor field for consistent ordering
        sort_field = cursor_field if cursor_field else "_id"
        
        # Paginate through results
        skip = 0
        while True:
            cursor = collection.find(query).sort(sort_field, 1).skip(skip).limit(self.page_size)
            documents = list(cursor)
            
            if not documents:
                break
                
            for document in documents:
                yield self.serialize_document(document)
            
            # If we got fewer documents than page_size, we're done
            if len(documents) < self.page_size:
                break
                
            skip += self.page_size

    def __del__(self):
        """Clean up MongoDB client connection."""
        if self._client:
            self._client.close()


class MongoCollection(MongoStream):
    """
    A stream for full refresh syncs of MongoDB collections.
    """

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None) -> Iterable[Mapping[str, Any]]:
        """
        Read all records from the MongoDB collection.
        """
        return super().read_records("full_refresh", cursor_field, stream_slice, stream_state)


class MongoCollectionIncremental(MongoStream):
    """
    An incremental stream for MongoDB collections.
    """

    def __init__(self, cursor_field: str = "_id", **kwargs):
        """
        Initialize the incremental MongoDB stream.

        Args:
            cursor_field (str): The field to use as cursor for incremental syncs.
        """
        super().__init__(**kwargs)
        self._cursor_field = cursor_field

    @property
    def cursor_field(self) -> str:
        """
        Get the cursor field for incremental syncs.

        Returns:
            str: The cursor field name.
        """
        return self._cursor_field

    @property
    def source_defined_cursor(self) -> bool:
        """
        Indicate that the cursor is defined by the source.

        Returns:
            bool: True if the cursor is source-defined.
        """
        return True

    @property
    def supports_incremental(self) -> bool:
        """
        Indicate that this stream supports incremental syncs.

        Returns:
            bool: True if incremental syncs are supported.
        """
        return True

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Update the stream state based on the latest record.

        Args:
            current_stream_state (MutableMapping[str, Any]): The current state of the stream.
            latest_record (Mapping[str, Any]): The latest record fetched.

        Returns:
            Mapping[str, Any]: The updated stream state.
        """
        current_cursor_value = current_stream_state.get(self.cursor_field)
        latest_cursor_value = latest_record.get(self.cursor_field)

        if current_cursor_value is None:
            return {self.cursor_field: latest_cursor_value}

        # For _id field (ObjectId), we need to compare as strings
        if self.cursor_field == "_id":
            if isinstance(latest_cursor_value, str) and isinstance(current_cursor_value, str):
                return {self.cursor_field: max(latest_cursor_value, current_cursor_value)}
            else:
                return {self.cursor_field: str(latest_cursor_value)}
        else:
            # For other fields, use regular comparison
            return {self.cursor_field: max(latest_cursor_value, current_cursor_value)}

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None) -> Iterable[Mapping[str, Any]]:
        """
        Read records incrementally from the MongoDB collection.
        """
        return super().read_records("incremental", self.cursor_field, stream_slice, stream_state)