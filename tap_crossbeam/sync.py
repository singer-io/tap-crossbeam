import re

import singer
from singer import metrics, metadata, Transformer
from singer.bookmarks import set_currently_syncing

from tap_crossbeam.discover import discover
from tap_crossbeam.endpoints import ENDPOINTS_CONFIG

LOGGER = singer.get_logger()

def nested_get(dic, path):
    for key in path:
        value = dic.get(key)
        if value is None or key == path[-1]:
            return value
        dic = value

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

def sync_endpoint(client,
                  catalog,
                  state,
                  required_streams,
                  selected_streams,
                  stream_name,
                  endpoint,
                  key_bag):
    persist = endpoint.get('persist', True)

    if persist:
        stream = catalog.get_stream(stream_name)
        schema = stream.schema.to_dict()
        mdata = metadata.to_map(stream.metadata)
        write_schema(stream)

    url = None
    path = endpoint['path'].format(**key_bag)
    while path or url:
        LOGGER.info('{} - Syncing: {}'.format(
                stream_name,
                path or url))

        params = None
        if path and not url:
            params = endpoint.get('params', {})

        data = client.request('GET',
                              path=path,
                              url=url,
                              params=params,
                              endpoint=stream_name)

        records = data.get(endpoint.get('data_key', 'items'))

        if not records:
            return

        with metrics.record_counter(stream_name) as counter:
            with Transformer() as transformer:
                for record in records:
                    if persist and stream_name in selected_streams:
                        record = {**record, **key_bag}
                        record_typed = transformer.transform(record,
                                                             schema,
                                                             mdata)
                        singer.write_record(stream_name, record_typed)
                        counter.increment()
                    if 'children' in endpoint:
                        child_key_bag = dict(key_bag)

                        if 'provides' in endpoint:
                            for dest_key, obj_path in endpoint['provides'].items():
                                if not isinstance(obj_path, list):
                                    obj_path = [obj_path]
                                child_key_bag[dest_key] = nested_get(record, obj_path)

                        for child_stream_name, child_endpoint in endpoint['children'].items():
                            if child_stream_name in required_streams:
                                sync_endpoint(client,
                                              catalog,
                                              state,
                                              required_streams,
                                              selected_streams,
                                              child_stream_name,
                                              child_endpoint,
                                              child_key_bag)

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

def normalize_name(name):
    return re.sub(r'[^a-z0-9\_]', '_', name.lower())

def _records_schema(page_records):
    field_names = set()
    for output in page_records:
        field_names = field_names.union(set(output.keys()))
    properties = {}
    for field_name in field_names:
        properties[field_name] = {'type': ['null', 'string']}
    return {
        'type': 'object',
        'additionalProperties': False,
        'properties': properties
    }


def _convert_record_item(item):
    with open("item.json", "w") as fp:
        import json
        json.dump(item, fp=fp)
    output = {
        '_record_id': item['record_id'],
        '_crossbeam_id': item['crossbeam_id'],
        '_updated_at': item['updated_at'],
    }
    for k, v in (item['master']['top_level'] or {}).items():
        output[normalize_name(k)] = v
    for k, v in (item['master']['owner'] or {}).items():
        output[normalize_name('Owner ' + k)] = v
    return output

def _partner_records_schema(page_records):
    field_names = set()
    for output in page_records:
        field_names = field_names.union(set(output.keys()))
    fixed_types = {
        '_partner_organization_id': {'type': 'integer'},
        '_population_ids': {'type': 'array', 'items': {'type': 'integer'}},
        '_population_names': {'type': 'array', 'items': {'type': 'string'}},
        '_partner_population_ids': {'type': 'array', 'items': {'type': 'integer'}},
        '_partner_population_names': {'type': 'array', 'items': {'type': 'string'}},
    }
    properties = {}
    for field_name in field_names:
        properties[field_name] = fixed_types.get(field_name, {'type': ['null', 'string']})
    return {
        'type': 'object',
        'additionalProperties': False,
        'properties': properties
    }

def _convert_partner_record_item(item, partner_lookup):
    # separate the accounts and leads into different streams?
    # No longer returning `master`, so not sure how to determine the type
    # id_key = '_lead_id' if item['master']['mdm_type'] == 'lead' else '_account_id'
    output = {
        '_record_id': item['record_id'],
        '_crossbeam_id': item['crossbeam_id'],
        '_population_ids': [x['id'] for x in item['populations']],
        '_population_names': [x['name'] for x in item['populations']],
        '_partner_organization_id': item['partner_organization_id'],
        '_partner_name': partner_lookup[item['partner_organization_id']],
        '_partner_crossbeam_id': item['partner_crossbeam_id'],
        '_partner_population_ids': [x['id'] for x in item['partner_populations']],
        '_partner_population_names': [x['name'] for x in item['partner_populations']],
    }
    for k, v in (item['partner_master']['top_level'] or {}).items():
        output[normalize_name(k)] = v
    for k, v in (item['partner_master']['owner'] or {}).items():
        output[normalize_name('Owner ' + k)] = v
    return output

def _write_metrics(stream_name, schema, page_records):
    with metrics.record_counter(stream_name) as counter:
        with Transformer() as transformer:
            for record in page_records:
                record_typed = transformer.transform(record,
                                                     schema,
                                                     [])
                singer.write_record(stream_name, record_typed)
                counter.increment()

def _partner_lookup(client):
    partners_response = client.request('GET',
                                       path='/v0.1/partners',
                                       endpoint='partners')
    partner_lookup = {}
    for partner in partners_response['partner_orgs']:
        partner_lookup[partner['id']] = partner['name']
    return partner_lookup

def sync_partner_records(client, state):
    first_page = True
    schema = None
    stream_name = 'partner_records'
    update_current_stream(state, stream_name)
    partner_lookup = _partner_lookup(client)
    url = None
    path = '/v0.1/partner-records?limit=1000'
    while path or url:
        LOGGER.info('{} - Syncing: {}'.format(
                stream_name,
                path or url))
        partner_records = client.request(
            'GET',
            path=path,
            url=url,
            endpoint=stream_name)
        page_records = [
            _convert_partner_record_item(item, partner_lookup)
            for item in partner_records['items']
        ]
        if first_page:
            schema = _partner_records_schema(page_records)
            singer.write_schema(stream_name, schema, ['_crossbeam_id', '_partner_crossbeam_id'])
            first_page = False
        _write_metrics(stream_name, schema, page_records)
        url = nested_get(partner_records, ['pagination', 'next_href'])
        path = None

CROSSBEAM_FIELDS_MAP = {
    '__xb_crossbeam_id': 'crossbeam_id',
    '__xb_record_id': 'record_id',
    '__xb_record_type': 'record_type',
    '__xb_record_name': 'record_name',
    '__xb_updated_at': 'updated_at'
}

MDM_MAP = {
    'company': 'account',
    'person': 'lead'
}

def sync_records(client, catalog, required_streams):
    md_type_lookup = {}
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        table_mdata = mdata.get(tuple())
        if table_mdata and table_mdata.get('tap-crossbeam.is_partner_data', False):
            display_name_lookup = {}

            for breadcrumb, meta in mdata.items():
                for display_name in meta.get('tap-crossbeam.display_names', []):
                    display_name_lookup[display_name] = breadcrumb[1]
                for nickname in meta.get('tap-crossbeam.nicknames', []):
                    display_name_lookup[nickname] = breadcrumb[1]

            md_type_lookup[table_mdata['tap-crossbeam.mdm_type']] = {
                'stream_name': stream.stream,
                'metadata': mdata,
                'schema': stream.schema.to_dict(),
                'stream': stream,
                'display_name_lookup': display_name_lookup,
                'schema_written': False
            }

    path = '/v0.1/records?limit=1000'
    next_href = None
    while path or next_href:
        LOGGER.info('records - Fetching {}'.format(path or next_href))
        data = client.get(path, url=next_href, endpoint='records')
        for raw_record in data['items']:
            record = {}
            correction = MDM_MAP[raw_record['record_type']]
            stream_lookup = md_type_lookup[correction or raw_record['record_type']]
            stream_name = stream_lookup['stream_name']

            if stream_name not in required_streams:
                continue

            if not stream_lookup['schema_written']:
                write_schema(stream)
                stream_lookup['schema_written'] = True

            for display_name, value in raw_record['master']['top_level'].items():
                field_name = stream_lookup['display_name_lookup'].get(display_name)
                if not field_name:
                    LOGGER.warning('"{}" not found'.format(display_name))
                else:
                    record[field_name] = value

            for singer_field, crossbeam_field in CROSSBEAM_FIELDS_MAP.items():
                record[singer_field] = raw_record[crossbeam_field]

            with Transformer() as transformer:
                record_typed = transformer.transform(record,
                                                     stream_lookup['schema'],
                                                     stream_lookup['metadata'])
                singer.write_record(stream_name, record_typed)

        path = None
        next_href = data.get('pagination', {}).get('next_href')

def sync(client, config, catalog, state):
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

    sync_partner_records(client, state)

    update_current_stream(state)
