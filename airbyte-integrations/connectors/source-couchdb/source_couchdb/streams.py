# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from abc import ABC
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
class IncrementalCouchdbStream(HttpStream, ABC):
    def __init__(self, url_base: str, page_size: int, *args, **kwargs):
        self.__url_base = url_base
        self.__page_size = page_size
        return super().__init__(*args, **kwargs)

    @property
    def url_base(self) -> str:
        return self.__url_base

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
        _changes api does not use pagination.
        """
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
        latest_cursor_value = latest_record.get("seq")

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
    cursor_field = "seq"

    # Define a chave primária para o stream
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "_changes"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        response_json = response.json()
        if "results" not in response_json:
            raise KeyError("Response does not contain 'results' field.")
        last_seq = response_json["last_seq"]
        for row in response_json["results"]:
            yield self._fetch_document(row=row, last_seq=last_seq)

    def _fetch_document(self, row: Mapping, last_seq: str = None) -> Mapping:
        doc_id = row["id"]
        url = f"{self.url_base}{doc_id}"
        self.logger.info(f"Fetching document: {url}")
        response = self._session.get(url)
        response.raise_for_status()
        doc = response.json()

        return {
            "id": doc.get("_id"),
            "key": doc.get("_id"),
            "value": {"rev": doc.get("_rev")},
            "doc": doc,
            "seq": row.get("seq"),
            "last_seq": last_seq,
        }

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Define os slices para o stream. Neste caso, não precisamos de slices específicos,
        então retornamos um único slice vazio.
        """
        return [None]
