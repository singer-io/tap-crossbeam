import re

import singer
from singer import metrics, metadata, Transformer
from singer.bookmarks import set_currently_syncing

from tap_crossbeam.discover import (
    RECORDS_STANDARD,
    PARTNER_RECORDS_STANDARD,
    discover,
    normalize_name,
)
from tap_crossbeam.endpoints import ENDPOINTS_CONFIG

LOGGER = singer.get_logger()


def nested_get(dic, path):
    for key in path:
        value = dic.get(key)
        if value is None or key == path[-1]:
            return value
        dic = value
    return None


def get_bookmark(state, stream_name, default):
    return state.get('bookmarks', {}).get(stream_name, default)


def write_bookmark(state, stream_name, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream_name] = value
    singer.write_state(state)


def write_schema(stream):
    schema = stream.schema.to_dict()
    singer.write_schema(stream.tap_stream_id, schema, stream.key_properties)
    return schema


def _child_key_bag(key_bag, endpoint, record):
    child_key_bag = key_bag.copy()
    for dest_key, obj_path in endpoint.get('provides', {}).items():
        if not isinstance(obj_path, list):
            obj_path = [obj_path]
        child_key_bag[dest_key] = nested_get(record, obj_path)
    return child_key_bag


def sync_endpoint(client,
                  catalog,
                  state,
                  required_streams,
                  selected_streams,
                  stream_name,
                  endpoint,
                  key_bag):
    stream = catalog.get_stream(stream_name)
    schema = write_schema(stream)
    url = None
    path = endpoint['path'].format(**key_bag)
    while path or url:
        LOGGER.info('%s - Syncing: %s', stream_name, path or url)
        params = None
        if path and not url:
            params = endpoint.get('params', {})
        data = client.request('GET', path=path, url=url, params=params, endpoint=stream_name)
        records = data.get(endpoint.get('data_key', 'items'))
        if not records:
            return
        if stream_name in selected_streams:
            _write_records_and_metrics(stream_name, schema, [
                {**record, **key_bag} for record in records])
        for child_stream_name, child_endpoint in endpoint.get('children', {}).items():
            if child_stream_name not in required_streams:
                continue
            for record in records:
                sync_endpoint(client,
                              catalog,
                              state,
                              required_streams,
                              selected_streams,
                              child_stream_name,
                              child_endpoint,
                              _child_key_bag(key_bag, endpoint, record))
        url = nested_get(data, ['pagination', 'next_href'])
        path = None


def update_current_stream(state, stream_name=None):
    set_currently_syncing(state, stream_name)
    singer.write_state(state)


def get_required_streams(endpoints, selected_stream_names):
    required_streams = []
    for name, endpoint in endpoints.items():
        child_required_streams = None
        if 'children' in endpoint:
            child_required_streams = get_required_streams(endpoint['children'],
                                                          selected_stream_names)
        if name in selected_stream_names or child_required_streams:
            required_streams.append(name)
            if child_required_streams:
                required_streams += child_required_streams
    return required_streams


def _write_records_and_metrics(stream_name, schema, page_records):
    with metrics.record_counter(stream_name) as counter:
        with Transformer() as transformer:
            for record in page_records:
                record_typed = transformer.transform(record, schema, [])
                singer.write_record(stream_name, record_typed)
                counter.increment()


def _partner_lookup(client):
    partners_response = client.request('GET', path='/v0.1/partners', endpoint='partners')
    partner_lookup = {}
    for partner in partners_response['partner_orgs']:
        partner_lookup[partner['id']] = partner['name']
    return partner_lookup


def _source_id_lookup(catalog):
    """Returns a mapping of source ids to the stream for that source."""
    lookup = {}
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        table_mdata = mdata.get(tuple())
        if table_mdata and table_mdata.get('tap-crossbeam.source_ids'):
            for source_id in table_mdata['tap-crossbeam.source_ids']:
                lookup[source_id] = {
                    'metadata': mdata,
                    'stream': stream,
                    'schema': stream.schema.to_dict(),
                }
    return lookup


def sync_partner_records(client, catalog, required_streams):
    for stream in catalog.streams:
        if stream.stream in ['partner_account', 'partner_user', 'partner_lead']:
            write_schema(stream)
    partner_lookup = _partner_lookup(client)
    source_id_lookup = _source_id_lookup(catalog)
    for raw_record in client.yield_partner_records():
        record = {}
        mdmeta = source_id_lookup[raw_record['partner_source_id']]
        stream_name = mdmeta['stream'].stream
        if stream_name not in required_streams:
            continue
        augmented_rec = {
            'population_ids': [x['id'] for x in raw_record['populations']],
            'population_names': [x['name'] for x in raw_record['populations']],
            'partner_name': partner_lookup[raw_record['partner_organization_id']],
            'partner_population_ids': [x['id'] for x in raw_record['partner_populations']],
            'partner_population_names': [x['name'] for x in raw_record['partner_populations']],
            **raw_record,
        }
        for display_name, value in augmented_rec['partner_master']['top_level'].items():
            # FIXME only do this if the field is selected, right?
            record[normalize_name(display_name)] = value
        # FIXME owner needs to be handled
        for field in PARTNER_RECORDS_STANDARD:
            record[field] = augmented_rec[field[1:]]
        with Transformer() as transformer:
            record_typed = transformer.transform(record, mdmeta['schema'], mdmeta['metadata'])
            singer.write_record(stream_name, record_typed)


def sync_records(client, catalog, required_streams):
    for stream in catalog.streams:
        if stream.stream in ['account', 'user', 'lead']:
            write_schema(stream)
    source_id_lookup = _source_id_lookup(catalog)
    for raw_record in client.yield_records():
        record = {}
        mdmeta = source_id_lookup[raw_record['source_id']]
        stream_name = mdmeta['stream'].stream
        if stream_name not in required_streams:
            continue
        for display_name, value in raw_record['master']['top_level'].items():
            # FIXME only do this if the field is selected, right?
            record[normalize_name(display_name)] = value
        # FIXME owner needs to be handled
        for field in RECORDS_STANDARD:
            record[field] = raw_record[field[1:]]
        with Transformer() as transformer:
            record_typed = transformer.transform(record, mdmeta['schema'], mdmeta['metadata'])
            singer.write_record(stream_name, record_typed)


def sync(client, _, catalog, state):
    if not catalog:
        catalog = discover(client)
        selected_streams = catalog.streams
    else:
        selected_streams = catalog.get_selected_streams(state)

    selected_stream_names = []
    for selected_stream in selected_streams:
        selected_stream_names.append(selected_stream.tap_stream_id)

    required_endpoint_streams = get_required_streams(ENDPOINTS_CONFIG, selected_stream_names)

    for stream_name, endpoint in ENDPOINTS_CONFIG.items():
        if stream_name in required_endpoint_streams:
            update_current_stream(state, stream_name)
            sync_endpoint(client,
                          catalog,
                          state,
                          required_endpoint_streams,
                          selected_stream_names,
                          stream_name,
                          endpoint,
                          {})

    # records data streams are interlaced, so we just call this stage "records"
    update_current_stream(state, 'records')
    sync_records(client, catalog, selected_stream_names)
    sync_partner_records(client, catalog, selected_stream_names)
    update_current_stream(state)
