# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from abc import ABC
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import requests

from airbyte_cdk.sources.streams.http import HttpStream


# Basic full refresh stream
class CouchdbStream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class CouchdbStream(HttpStream, ABC)` which is the current class
    `class Customers(CouchdbStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(CouchdbStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalCouchdbStream((CouchdbStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

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
    primary_key = "key"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "_all_docs"


# Basic incremental stream
class IncrementalCouchdbStream(CouchdbStream, ABC):
    """
    Base class for CouchDB streams that support incremental sync.
    """

    # Define o intervalo de checkpoint para salvar o estado após N registros
    state_checkpoint_interval = 1000

    @property
    def cursor_field(self) -> str:
        """
        Retorna o campo que será usado como cursor para a sincronização incremental.
        Este campo deve ser um campo que aumenta monotonicamente, como um timestamp ou um ID.
        """
        raise NotImplementedError("Subclasses should implement this method to return the cursor field.")

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """
        Atualiza o estado do stream com base no registro mais recente.
        O estado é usado para determinar a partir de onde a próxima sincronização deve começar.
        """
        current_cursor_value = current_stream_state.get(self.cursor_field, None)
        latest_cursor_value = latest_record.get(self.cursor_field)

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
        Adiciona parâmetros de consulta para suportar a sincronização incremental.
        """
        params = super().request_params(stream_state, stream_slice, next_page_token)
        if stream_state:
            params["startkey"] = stream_state.get(self.cursor_field)
        return params


class DocumentsIncremental(IncrementalCouchdbStream):
    """
    Stream incremental para documentos no CouchDB.
    """

    # Define o campo que será usado como cursor para a sincronização incremental
    cursor_field = "last_modified"

    # Define a chave primária para o stream
    primary_key = "key"

    def path(self, **kwargs) -> str:
        """
        Retorna o caminho da API que será usado para buscar os documentos.
        """
        return "_all_docs"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Parseia a resposta da API e retorna os registros.
        """
        response_json = response.json()
        if "rows" not in response_json:
            raise KeyError("Response does not contain 'rows' field.")
        for row in response_json["rows"]:
            yield row["doc"]

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Define os slices para o stream. Neste caso, não precisamos de slices específicos,
        então retornamos um único slice vazio.
        """
        return [None]

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """
        Atualiza o estado do stream com base no registro mais recente.
        """
        current_cursor_value = current_stream_state.get(self.cursor_field, None)
        latest_cursor_value = latest_record.get(self.cursor_field)

        if current_cursor_value is None:
            return {self.cursor_field: latest_cursor_value}
        else:
            return {self.cursor_field: max(latest_cursor_value, current_cursor_value)}
