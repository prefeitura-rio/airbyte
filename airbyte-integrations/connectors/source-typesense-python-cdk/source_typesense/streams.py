# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from abc import ABC
from typing import Any, Iterable, Mapping, Optional

from typesense import Client

from airbyte_cdk.sources.streams import Stream


class TypesenseStream(Stream, ABC):
    """
    A base class for Typesense streams. This class provides common functionality for interacting with Typesense collections.
    """

    def __init__(self, collection_name: str, protocol: str, host: str, port: int, api_key: str, page_size: int, **kwargs):
        """
        Initialize the TypesenseStream.

        Args:
            collection_name (str): The name of the Typesense collection.
            protocol (str): The protocol (http or https).
            host (str): The Typesense hostname.
            port (int): The Typesense port.
            api_key (str): The Typesense API key.
            page_size (int): The number of records to fetch per page.
        """
        super().__init__(**kwargs)
        self.collection_name = collection_name
        self.protocol = protocol
        self.host = host
        self.port = port
        self.api_key = api_key
        self.page_size = page_size
        self._client = None

    @property
    def name(self) -> str:
        """
        Get the name of the stream.

        Returns:
            str: The stream name (collection name).
        """
        return self.collection_name

    @property
    def primary_key(self) -> Optional[str]:
        """
        Get the primary key for the stream.

        Returns:
            str: The primary key field name.
        """
        return "id"

    def get_client(self) -> Client:
        """
        Get or create a Typesense client.

        Returns:
            Client: The Typesense client instance.
        """
        if self._client is None:
            self._client = Client({
                'nodes': [{
                    'host': self.host,
                    'port': str(self.port),
                    'protocol': self.protocol
                }],
                'api_key': self.api_key,
                'connection_timeout_seconds': 30
            })
        return self._client

    def serialize_document(self, document: dict) -> dict:
        """
        Serialize Typesense document for Airbyte.
        Restructures document as {"id": "...", "data": {...}}

        Args:
            document (dict): The Typesense document.

        Returns:
            dict: The serialized document with id and data fields.
        """
        # Extract id field
        doc_id = document.get("id")
        if doc_id is None:
            # Skip documents without id (should be very rare, but prevents null values)
            return None

        # Create a copy without id for the data field
        data = {k: v for k, v in document.items() if k != "id"}

        return {
            "id": str(doc_id),
            "data": data
        }

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None) -> Iterable[Mapping[str, Any]]:
        """
        Read records from the Typesense collection using export endpoint.

        Args:
            sync_mode: The sync mode (full_refresh only for now).
            cursor_field: The cursor field (not used for full refresh).
            stream_slice: The stream slice.
            stream_state: The current stream state.

        Returns:
            Iterable[Mapping[str, Any]]: An iterable of records.
        """
        client = self.get_client()

        try:
            # Use the export endpoint which returns JSONL (one JSON per line)
            # This is more efficient than using search with pagination
            export_response = client.collections[self.collection_name].documents.export()

            # The export response is a string with JSONL format
            # Each line is a JSON document
            import json
            for line in export_response.strip().split('\n'):
                if not line:
                    continue

                try:
                    document = json.loads(line)
                    serialized = self.serialize_document(document)
                    if serialized is not None:
                        yield serialized
                except json.JSONDecodeError:
                    # Skip invalid JSON lines
                    continue

        except Exception as e:
            raise Exception(f"Failed to export documents from collection {self.collection_name}: {str(e)}")


class TypesenseCollection(TypesenseStream):
    """
    A stream for full refresh syncs of Typesense collections.
    """
    pass
