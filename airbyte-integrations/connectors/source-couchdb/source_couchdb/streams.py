# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from abc import ABC
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import requests

from airbyte_cdk.sources.streams.http import HttpStream


# Basic full refresh stream
class CouchdbStream(HttpStream, ABC):
    def __init__(self, url_base: str, page_size: int, *args, **kwargs):
        self.__url_base = url_base
        self.__page_size = page_size
        return super().__init__(*args, **kwargs)

    @property
    def url_base(self) -> str:
        return self.__url_base

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Builds query parameters needed to get the next page in the response.

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        response_json = response.json()
        offset = response_json.get("offset", 0)
        total_fetched = offset + self.__page_size
        try:
            total_rows = response_json["total_rows"]
        except KeyError:
            raise KeyError("Response does not contain 'total_rows' field.")
        if total_fetched < total_rows:
            return {
                "skip": total_fetched,
            }
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        base_params = {
            "include_docs": True,
            "limit": self.__page_size,
        }
        if next_page_token:
            base_params.update(next_page_token)
        return base_params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        if "rows" not in response_json:
            raise KeyError("Response does not contain 'rows' field.")
        for row in response_json["rows"]:
            yield row


class Documents(CouchdbStream):
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "_all_docs"


# Basic incremental stream
class IncrementalCouchdbStream(CouchdbStream, ABC):
    state_checkpoint_interval = 1000

    @property
    def cursor_field(self) -> str:
        """
        Retorna o campo que será usado como cursor para a sincronização incremental.
        Este campo deve ser um campo que aumenta monotonicamente, como um timestamp ou um ID.
        """
        raise NotImplementedError("Subclasses should implement this method to return the cursor field.")

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        _changes api  uses the last_seq to control the pagination
        if pending is not 0 there are more data, return the next_page_token.
        """
        response_json = response.json()
        pending = response_json.get("pending", 0)
        last_seq = response_json.get("last_seq")
        self.logger.info(f"Pending: {pending}")
        if pending > 0 and last_seq:
            return {"since": last_seq}
        return None

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """
        Updates the stream state based on the most recent record.
        """
        current_cursor_value = current_stream_state.get(self.cursor_field, None)
        latest_cursor_value = latest_record.get("last_seq")

        if current_cursor_value is None:
            return {self.cursor_field: latest_cursor_value}
        else:
            return {self.cursor_field: max(latest_cursor_value, current_cursor_value)}

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        """
        Adds query parameters to support incremental sync with _changes.
        """
        params = super().request_params(stream_state, stream_slice, next_page_token)
        if stream_state:
            params["since"] = stream_state.get(self.cursor_field)
        params["feed"] = "normal"
        return params

    def _send_request(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> requests.Response:
        """
        Sends the request and logs the url.
        """
        self.logger.info(f"Request URL: {request.url}")
        return super()._send_request(request, request_kwargs)


class DocumentsIncremental(IncrementalCouchdbStream):
    """
    Stream incremental para documentos no CouchDB usando _changes.
    """

    # Define o campo que será usado como cursor para a sincronização incremental
    cursor_field = "last_seq"

    # Define a chave primária para o stream
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "_changes"

    def process_document(self, result, rows, last_seq):
        if "docs" not in result or not result["docs"]:
            self.logger.warning(f"No 'docs' field in result: {result}")
            return None

        # Extract the document from the nested structure
        doc = result["docs"][0].get("ok", {})
        if "_id" not in doc:
            self.logger.warning(f"Document missing '_id' field: {doc}")
            return None

        # Match the document with the corresponding row to get the sequence number
        seq = next((row["seq"] for row in rows if row["id"] == doc["_id"]), None)
        if seq is None:
            self.logger.warning(f"No matching row found for document: {doc}")
            return None

        return {
            "id": doc.get("_id"),
            "key": doc.get("_id"),
            "value": {"rev": doc.get("_rev")},
            "doc": doc,
            "seq": seq,
            "last_seq": last_seq,
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        if "results" not in response_json:
            raise KeyError("Response does not contain 'results' field.")
        last_seq = response_json["last_seq"]
        rows = response_json["results"]

        # Prepare bulk request
        bulk_docs = {"docs": [{"id": row["id"]} for row in rows]}
        url = f"{self.url_base}_bulk_get"
        bulk_response = self._session.post(url, json=bulk_docs)

        bulk_response.raise_for_status()
        bulk_docs_response = bulk_response.json()

        # Use ThreadPoolExecutor to process documents in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.process_document, result, rows, last_seq) for result in bulk_docs_response["results"]]

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    yield result

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Define os slices para o stream. Neste caso, não precisamos de slices específicos,
        então retornamos um único slice vazio.
        """
        return [None]
