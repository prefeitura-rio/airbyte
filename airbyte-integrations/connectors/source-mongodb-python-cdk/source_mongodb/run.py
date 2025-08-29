#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch

from .source import SourceMongodb


def run():
    source = SourceMongodb()
    launch(source, sys.argv[1:])
