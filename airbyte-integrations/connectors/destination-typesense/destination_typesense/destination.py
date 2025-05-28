#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import io
import logging
import time
from dataclasses import dataclass
from logging import getLogger
from typing import Any, Iterable, Mapping, cast

import orjson
from serpyco_rs import Serializer
from typesense import Client
from typing_extensions import override

from airbyte_cdk.destinations import Destination
from airbyte_cdk.exception_handler import init_uncaught_exception_handler
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteStateMessage,
    ConfiguredAirbyteCatalog,
    DestinationSyncMode,
    Status,
    Type,
)
from airbyte_cdk.models.airbyte_protocol_serializers import custom_type_resolver
from destination_typesense.writer import TypesenseWriter


logger = getLogger("airbyte")

@dataclass
class PatchedAirbyteStateMessage(AirbyteStateMessage):
    """Declare the `id` attribute that platform sends."""

    id: int | None = None
    """Injected by the platform."""


@dataclass
class PatchedAirbyteMessage(AirbyteMessage):
    """Keep all defaults but override the type used in `state`."""

    state: PatchedAirbyteStateMessage | None = None
    """Override class for the state message only."""


PatchedAirbyteMessageSerializer = Serializer(
    PatchedAirbyteMessage,
    omit_none=True,
    custom_type_resolver=custom_type_resolver,
)
"""Redeclared SerDes class using the patched dataclass."""


def get_client(config: Mapping[str, Any]) -> Client:
    hosts = config.get("host").split(",")
    path = config.get("path")
    nodes = []
    for host in hosts:
        node = {
            "host": host,
            "port": config.get("port") or "8108",
            "protocol": config.get("protocol") or "https",
        }
        if path:
            node["path"] = path
        nodes.append(node)
    client = Client(
        {
            "api_key": config.get("api_key"),
            "nodes": nodes,
            "connection_timeout_seconds": 3600,
        }
    )

    return client


class DestinationTypesense(Destination):
    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        client = get_client(config=config)

        for configured_stream in configured_catalog.streams:
            steam_name = configured_stream.stream.name
            if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
                try:
                    client.collections[steam_name].delete()
                except Exception:
                    pass
                client.collections.create({"name": steam_name, "fields": [{"name": ".*", "type": "auto"}]})

        writer = TypesenseWriter(client, config.get("batch_size"))
        for message in input_messages:
            if message.type == Type.STATE:
                writer.flush()
                yield message
            elif message.type == Type.RECORD:
                record = message.record
                writer.queue_write_operation(record.stream, record.data)
            else:
                continue
        writer.flush()

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        logger.debug("TypeSense Destination Config Check")
        try:
            client = get_client(config=config)
            client.collections.create({"name": "_airbyte", "fields": [{"name": "title", "type": "string"}]})

            writer = TypesenseWriter(client, config.get("batch_size", 10000))
            writer.queue_write_operation("_airbyte", {"id": "1", "title": "The Hunger Games"})
            writer.flush()

            time.sleep(3)
            client.collections["_airbyte"].documents["1"].retrieve()

            status = AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            status = AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
        finally:
            try:
                client = get_client(config=config)
                client.collections["_airbyte"].delete()
            except Exception:
                logger.warning("Failed to delete _airbyte collection")

        return status

    @override
    def run(self, args: list[str]) -> None:
        """Overridden from CDK base class in order to use the patched SerDes class."""
        init_uncaught_exception_handler(logger)
        parsed_args = self.parse_args(args)
        output_messages = self.run_cmd(parsed_args)
        for message in output_messages:
            print(
                orjson.dumps(
                    PatchedAirbyteMessageSerializer.dump(
                        cast(PatchedAirbyteMessage, message),
                    )
                ).decode()
            )

    @override
    def _parse_input_stream(self, input_stream: io.TextIOWrapper) -> Iterable[AirbyteMessage]:
        """Reads from stdin, converting to Airbyte messages.

        Includes overrides that should be in the CDK but we need to test it in the wild first.

        Rationale:
            The platform injects `id` but our serializer classes don't support
            `additionalProperties`.
        """
        for line in input_stream:
            try:
                yield PatchedAirbyteMessageSerializer.load(orjson.loads(line))
            except orjson.JSONDecodeError:
                logger.info(f"ignoring input which can't be deserialized as Airbyte Message: {line}")