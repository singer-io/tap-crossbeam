#!/usr/bin/env python3

import sys
import json
import argparse

import singer
from singer import metadata

from tap_crossbeam.client import CrossbeamClient
from tap_crossbeam.discover import discover
from tap_crossbeam.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'organization_uuid'
]

def do_discover(client):
    LOGGER.info('Testing authentication')
    try:
        client.get('/v0.1/users/me')
    except Exception as exc:
        raise Exception('Error could not authenticate with Crossbeam') from exc

    LOGGER.info('Starting discover')
    catalog = discover(client)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')

@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with CrossbeamClient(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover(client)
        else:
            sync(client,
                 parsed_args.config,
                 parsed_args.catalog,
                 parsed_args.state)
