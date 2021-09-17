import singer
from singer import metrics, metadata, Transformer
import singer.bookmarks as books
from tap_crossbeam.discover import (
    STANDARD_KEYS,
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
    books.set_currently_syncing(state, stream_name)
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


def _stream_to_meta_and_stream(stream):
    return {
        'metadata': metadata.to_map(stream.metadata),
        'stream': stream,
        'schema': stream.schema.to_dict(),
    }


def _stream_lookup(catalog):
    """Returns a mapping of stream names to the stream and metadata for that
    stream."""
    lookup = {}
    for stream in catalog.streams:
        meta_and_stream = _stream_to_meta_and_stream(stream)
        lookup[stream.stream] = meta_and_stream
    return lookup


def _write_owner(raw_record, user_mdmeta, master_key):
    record = {}
    for display_name, value in raw_record[master_key]['owner'].items():
        record[normalize_name(display_name)] = value
    for field in STANDARD_KEYS[user_mdmeta['stream'].stream]:
        record[field] = raw_record[field[1:]]
    with Transformer() as transformer:
        record_typed = transformer.transform(
            record, user_mdmeta['schema'], user_mdmeta['metadata'])
        singer.write_record(user_mdmeta['stream'].stream, record_typed)


def sync_partner_records(client, catalog, required_streams, state):
    for stream in catalog.streams:
        if stream.stream in ['partner_account', 'partner_user', 'partner_lead']:
            write_schema(stream)
    partner_lookup = {x['id']: x for x in client.yield_partners()}
    stream_lookup = _stream_lookup(catalog)
    user_stream = next((stream for stream in catalog.streams if stream.stream == 'partner_user'),
                       None)
    user_mdmeta = _stream_to_meta_and_stream(user_stream) if user_stream else None
    max_overlap_time = ''
    book_overlap_time = books.get_bookmark(state, 'partner_records', 'overlap_time', '')
    for raw_record in client.yield_partner_records():
        overlap_time = raw_record['overlap_time'] or ''
        if overlap_time and overlap_time < book_overlap_time:
            continue
        max_overlap_time = overlap_time if overlap_time > max_overlap_time else max_overlap_time
        if 'mdm_type' in raw_record:
            # FIXME remove once route is updated
            raw_record['partner_mdm_type'] = raw_record['mdm_type']
        mdmeta = stream_lookup['partner_' + raw_record['partner_mdm_type']]
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
        record = {}
        for display_name, value in augmented_rec['partner_master']['top_level'].items():
            record[normalize_name(display_name)] = value
        for field in STANDARD_KEYS[stream_name]:
            record[field] = augmented_rec[field[1:]]
        with Transformer() as transformer:
            record_typed = transformer.transform(record, mdmeta['schema'], mdmeta['metadata'])
            singer.write_record(stream_name, record_typed)
        if user_mdmeta and 'owner' in raw_record['partner_master']:
            _write_owner(raw_record, user_mdmeta, 'partner_master')
    books.write_bookmark(state, 'partner_records', 'overlap_time', max_overlap_time)
    singer.write_state(state)


def sync_records(client, catalog, required_streams, state):
    for stream in catalog.streams:
        if stream.stream in ['account', 'user', 'lead']:
            write_schema(stream)
    user_stream = next((stream for stream in catalog.streams if stream.stream == 'user'), None)
    user_mdmeta = _stream_to_meta_and_stream(user_stream) if user_stream else None
    source_lookup = {x['id']: x for x in client.yield_sources()}
    stream_lookup = _stream_lookup(catalog)
    max_updated_at = ''
    book_updated_at = books.get_bookmark(state, 'records', 'updated_at', '')
    for raw_record in client.yield_records():
        updated_at = raw_record['updated_at']
        if updated_at < book_updated_at:
            continue
        max_updated_at = updated_at if updated_at > max_updated_at else max_updated_at
        source = source_lookup[raw_record['source_id']]
        mdmeta = stream_lookup[source['mdm_type']]
        stream_name = mdmeta['stream'].stream
        if stream_name not in required_streams:
            continue
        record = {}
        for display_name, value in raw_record['master']['top_level'].items():
            record[normalize_name(display_name)] = value
        for field in STANDARD_KEYS[stream_name]:
            record[field] = raw_record[field[1:]]
        with Transformer() as transformer:
            record_typed = transformer.transform(record, mdmeta['schema'], mdmeta['metadata'])
            singer.write_record(stream_name, record_typed)
        if user_mdmeta and 'owner' in raw_record['master']:
            _write_owner(raw_record, user_mdmeta, 'master')
    books.write_bookmark(state, 'records', 'updated_at', max_updated_at)
    singer.write_state(state)


def sync(client, _, catalog, state):
    if catalog:
        selected_streams = catalog.get_selected_streams(state)
    else:
        catalog = discover(client)
        selected_streams = catalog.streams
    currently_syncing = books.get_currently_syncing(state)
    selected_stream_names = []
    for selected_stream in sorted(selected_streams, key=lambda s: s.tap_stream_id):
        selected_stream_names.append(selected_stream.tap_stream_id)
    required_endpoint_streams = get_required_streams(ENDPOINTS_CONFIG, selected_stream_names)
    for stream_name, endpoint in ENDPOINTS_CONFIG.items():
        if currently_syncing:
            if currently_syncing == stream_name:
                currently_syncing = None
            else:
                continue
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
    if not currently_syncing or currently_syncing == 'records':
        currently_syncing = None
        update_current_stream(state, 'records')
        sync_records(client, catalog, selected_stream_names, state)
    if not currently_syncing or currently_syncing == 'partner_records':
        currently_syncing = None
        update_current_stream(state, 'partner_records')
        sync_partner_records(client, catalog, selected_stream_names, state)
    update_current_stream(state)
