# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import requests

from airbyte_cdk.sources.streams.http import HttpStream


class CouchdbStream(HttpStream, ABC):
    """
    A base class for CouchDB streams. This class provides common functionality for interacting with CouchDB APIs.
    """

    def __init__(self, url_base: str, page_size: int, *args, **kwargs):
        """
        Initialize the CouchdbStream.

        Args:
            url_base (str): The base URL for the CouchDB instance.
            page_size (int): The number of records to fetch per page.
        """
        self._url_base = url_base
        self._page_size = page_size
        super().__init__(*args, **kwargs)

    @property
    def url_base(self) -> str:
        """
        Get the base URL for the CouchDB instance.

        Returns:
            str: The base URL.
        """
        return self._url_base

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Builds query parameters needed to get the next page in the response.

        Args:
            response (requests.Response): The most recent response from the API.

        Returns:
            Optional[Mapping[str, Any]]: A mapping containing information needed to query the next page in the response.
                                         If there are no more pages, return None.
        """
        response_json = response.json()
        offset = response_json.get("offset", 0)
        total_fetched = offset + self._page_size
        total_rows = response_json.get("total_rows")

        if total_rows is None:
            raise KeyError("Response does not contain 'total_rows' field.")

        return {"skip": total_fetched} if total_fetched < total_rows else None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        """
        Build request parameters for the API call.

        Args:
            stream_state (Mapping[str, Any]): The current state of the stream.
            stream_slice (Mapping[str, Any], optional): A slice of the stream to fetch. Defaults to None.
            next_page_token (Mapping[str, Any], optional): Token for the next page. Defaults to None.

        Returns:
            MutableMapping[str, Any]: A dictionary of request parameters.
        """
        params = {"include_docs": True, "limit": self._page_size}
        if next_page_token:
            params.update(next_page_token)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Parse the API response and yield each record.

        Args:
            response (requests.Response): The API response.

        Returns:
            Iterable[Mapping]: An iterable containing each record in the response.
        """
        response_json = response.json()
        if "rows" not in response_json:
            raise KeyError("Response does not contain 'rows' field.")
        return response_json["rows"]


class Documents(CouchdbStream):
    """
    A stream for fetching all documents from a CouchDB database.
    """

    primary_key = "id"

    def path(self, **kwargs) -> str:
        """
        Get the path for the API endpoint.

        Returns:
            str: The API endpoint path.
        """
        return "_all_docs"


class IncrementalCouchdbStream(CouchdbStream, ABC):
    """
    A base class for incremental CouchDB streams. This class provides functionality for incremental syncs.
    """

    state_checkpoint_interval = 1000

    @property
    @abstractmethod
    def cursor_field(self) -> str:
        """
        Get the cursor field for incremental sync.

        Returns:
            str: The cursor field name.
        """
        raise NotImplementedError("Subclasses should implement this method to return the cursor field.")

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Builds query parameters needed to get the next page in the response for incremental sync.

        Args:
            response (requests.Response): The most recent response from the API.

        Returns:
            Optional[Mapping[str, Any]]: A mapping containing information needed to query the next page in the response.
                                         If there are no more pages, return None.
        """
        response_json = response.json()
        pending = response_json.get("pending", 0)
        last_seq = response_json.get("last_seq")
        self.logger.info(f"Pending: {pending}")
        # self.logger.info(f"last_seq: {last_seq}")
        if pending > 0 and last_seq:
            return {"since": last_seq}
        return None

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """
        Update the stream state based on the most recent record.

        Args:
            current_stream_state (MutableMapping[str, Any]): The current state of the stream.
            latest_record (Mapping[str, Any]): The most recent record fetched.

        Returns:
            Mapping[str, Any]: The updated stream state.
        """
        current_cursor_value = current_stream_state.get(self.cursor_field)
        latest_cursor_value = latest_record.get(self.cursor_field)

        # self.logger.info(f"current_cursor_value: {current_cursor_value}")
        self.logger.info(f"latest_cursor_value: {latest_cursor_value}")

        if current_cursor_value is None:
            return {self.cursor_field: latest_cursor_value}
        return {self.cursor_field: max(latest_cursor_value, current_cursor_value)}

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        """
        Build request parameters for the API call for incremental sync.

        Args:
            stream_state (Mapping[str, Any]): The current state of the stream.
            stream_slice (Mapping[str, Any], optional): A slice of the stream to fetch. Defaults to None.
            next_page_token (Mapping[str, Any], optional): Token for the next page. Defaults to None.

        Returns:
            MutableMapping[str, Any]: A dictionary of request parameters.
        """
        params = super().request_params(stream_state, stream_slice, next_page_token)
        params["feed"] = "normal"
        if stream_state:
            params["since"] = stream_state.get(self.cursor_field)
        return params

    def _send_request(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> requests.Response:
        """
        Send the request and log the URL.

        Args:
            request (requests.PreparedRequest): The prepared request to send.
            request_kwargs (Mapping[str, Any]): Additional keyword arguments for the request.

        Returns:
            requests.Response: The response from the API.
        """
        self.logger.info(f"Request URL: {request.url}")
        return super()._send_request(request, request_kwargs)


class DocumentsIncremental(IncrementalCouchdbStream):
    """
    An incremental stream for fetching documents from a CouchDB database using the _changes API.
    """

    cursor_field = "last_seq"
    primary_key = "id"

    def path(self, **kwargs) -> str:
        """
        Get the path for the API endpoint.

        Returns:
            str: The API endpoint path.
        """
        return "_changes"

    def process_document(
        self,
        result: Mapping[str, Any],
        rows: Iterable[Mapping[str, Any]],
        last_seq: str,
    ) -> Optional[Mapping[str, Any]]:
        """
        Process a single document from the bulk response.

        Args:
            result (Mapping[str, Any]): A result from the bulk response.
            rows (Iterable[Mapping[str, Any]]): The rows from the _changes response.
            last_seq (str): The last sequence number from the _changes response.

        Returns:
            Optional[Mapping[str, Any]]: A processed document or None if the document is invalid.
        """
        docs = result.get("docs", [])
        if not docs:
            self.logger.warning(f"No 'docs' field in result: {result}")
            return None

        doc = docs[0].get("ok", {})
        if "_id" not in doc:
            self.logger.warning(f"Document missing '_id' field: {doc}")
            return None

        seq = next((row["seq"] for row in rows if row["id"] == doc["_id"]), None)
        if seq is None:
            self.logger.warning(f"No matching row found for document: {doc}")
            return None

        return {
            "id": doc["_id"],
            "key": doc["_id"],
            "value": {"rev": doc.get("_rev")},
            "doc": doc,
            "seq": seq,
            "last_seq": last_seq,
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Parse the API response and yield each record.

        Args:
            response (requests.Response): The API response.

        Returns:
            Iterable[Mapping]: An iterable containing each record in the response.
        """
        response_json = response.json()
        if "results" not in response_json:
            raise KeyError("Response does not contain 'results' field.")
        last_seq = response_json.get("last_seq")
        rows = response_json["results"]

        bulk_docs = {"docs": [{"id": row["id"]} for row in rows]}
        url = f"{self.url_base}_bulk_get"
        bulk_response = self._session.post(url, json=bulk_docs)
        bulk_response.raise_for_status()
        bulk_docs_response = bulk_response.json()

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.process_document, result, rows, last_seq) for result in bulk_docs_response["results"]]
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    yield result

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Define slices for the stream. In this case, no specific slices are needed.

        Args:
            stream_state (Mapping[str, Any], optional): The current state of the stream. Defaults to None.

        Returns:
            Iterable[Optional[Mapping[str, Any]]]: A list of stream slices.
        """
        return [None]
