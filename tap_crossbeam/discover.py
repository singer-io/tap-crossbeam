import os
import re
import json

from singer.catalog import Catalog, CatalogEntry, Schema

from tap_crossbeam.endpoints import ENDPOINTS_CONFIG


def get_pk(stream_name, endpoints=None):
    if not endpoints:
        endpoints = ENDPOINTS_CONFIG
    for endpoint_stream_name, endpoint in endpoints.items():
        if stream_name == endpoint_stream_name:
            return endpoint['pk']
        if 'children' in endpoint:
            pk = get_pk(stream_name, endpoints=endpoint['children'])
            if pk:
                return pk
    return None


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas():
    schemas = {}
    field_metadata = {}

    schemas_path = get_abs_path('schemas')
    file_names = [f for f in os.listdir(schemas_path)
                  if os.path.isfile(os.path.join(schemas_path, f))]
    for file_name in file_names:
        stream_name = file_name[:-5]
        with open(os.path.join(schemas_path, file_name), encoding="utf-8") as data_file:
            schema = json.load(data_file)
        schemas[stream_name] = schema
        pk = get_pk(stream_name)
        metadata = []
        for prop, _ in schema['properties'].items():
            if prop in pk:
                inclusion = 'automatic'
            else:
                inclusion = 'available'
            metadata.append({
                'metadata': {'inclusion': inclusion},
                'breadcrumb': ['properties', prop],
            })
        field_metadata[stream_name] = metadata
    return schemas, field_metadata


def _field_jschema_type(field):
    cb_type = field['data_type']
    pg_type = field['pg_data_type']
    if cb_type == 'datetime':
        return ('string', 'date-time')
    if cb_type == 'number':
        if pg_type in []:
            return ('integer', None)
        return ('number', None)
    if cb_type == 'boolean':
        return ('boolean', None)
    return ('string', None)


def normalize_name(name):
    return re.sub(r'[^a-z0-9\_]', '_', name.lower())


def _add_field_to_properties(stream, field):
    column_name = normalize_name(field['display_name'])
    json_type, json_format = _field_jschema_type(field)
    if column_name in stream['properties']:
        if json_type not in stream['properties'][column_name]['type']:
            # Here we are handling the case when there are multiple different
            # types for this field. This could happen if, for example, one data
            # source has a field called 'Number of Employees' which is an
            # integer and another has 'Number of Employees' which is a string.
            stream['properties'][column_name]['type'] = ['null', 'string']
    else:
        json_schema = {'type': ['null', json_type]}
        if json_format:
            json_schema['format'] = json_format
        stream['properties'][column_name] = json_schema


def _add_field_to_metadata(stream, field):
    column_name = normalize_name(field['display_name'])
    if column_name not in stream['metadata']:
        stream['metadata'][column_name] = {'inclusion': 'available'}


STRING = {'type': ['string']}
NULLABLE_STRING = {'type': ['null', 'string']}
NULLABLE_DATETIME = {'type': ['null', 'string'], 'format': 'date-time'}
INTEGER = {'type': ['integer']}
INTEGER_ARRAY = {'type': 'array', 'items': {'type': 'integer'}}
STRING_ARRAY = {'type': 'array', 'items': {'type': 'string'}}
STANDARD_KEYS = {
    'account': {
        '_crossbeam_id': STRING,
        '_record_id': STRING,
        '_updated_at': NULLABLE_DATETIME,
    },
    'lead': {
        '_crossbeam_id': STRING,
        '_record_id': STRING,
        '_updated_at': NULLABLE_DATETIME,
    },
    'user': {
        '_crossbeam_id': STRING,
    },
    'partner_account': {
        '_crossbeam_id': STRING,
        '_partner_crossbeam_id': STRING,
        '_partner_name': STRING,
        '_partner_organization_id': INTEGER,
        '_partner_population_ids': INTEGER_ARRAY,
        '_partner_population_names': STRING_ARRAY,
        '_population_ids': INTEGER_ARRAY,
        '_population_names': STRING_ARRAY,
        '_record_id': STRING,
    },
    'partner_lead': {
        '_crossbeam_id': STRING,
        '_partner_crossbeam_id': STRING,
        '_partner_name': STRING,
        '_partner_organization_id': INTEGER,
        '_partner_population_ids': INTEGER_ARRAY,
        '_partner_population_names': STRING_ARRAY,
        '_population_ids': INTEGER_ARRAY,
        '_population_names': STRING_ARRAY,
        '_record_id': STRING,
    },
    'partner_user': {
        '_partner_crossbeam_id': STRING,
    },
}
PRIMARY_KEYS = {
    'account': ['_crossbeam_id'],
    'lead': ['_crossbeam_id'],
    'user': ['_crossbeam_id'],
    'partner_account': ['_crossbeam_id', '_partner_crossbeam_id'],
    'partner_lead': ['_crossbeam_id', '_partner_crossbeam_id'],
    'partner_user': ['_partner_crossbeam_id'],
}


def _initialize_stream(streams, stream_name):
    if stream_name in streams:
        return
    standard = STANDARD_KEYS.get(stream_name, {}).copy()
    streams[stream_name] = {
        'properties': standard,
        'metadata': {'__table__': {}},
    }
    for column_name in standard:
        streams[stream_name]['metadata'][column_name] = {'inclusion': 'automatic'}


def _records_streams(client):
    streams = {}
    for source in client.yield_sources():
        if not source['mdm_type']:
            continue
        if source['mdm_type'] not in ['account', 'lead', 'user']:
            continue
        stream_name = source['mdm_type']
        _initialize_stream(streams, stream_name)
        fields = [
            field for field in source['fields']
            if field['is_primary_key'] or field['is_visible'] or field['is_filterable']
        ]
        for field in fields:
            _add_field_to_properties(streams[stream_name], field)
        for field in fields:
            _add_field_to_metadata(streams[stream_name], field)
    return streams


def _partner_records_streams(client):
    streams = {}
    for shared_field in client.yield_partner_shared_fields():
        if not shared_field['mdm_type']:
            continue
        if shared_field['mdm_type'] not in ['account', 'lead', 'user']:
            continue
        stream_name = 'partner_' + shared_field['mdm_type']
        _initialize_stream(streams, stream_name)
        _add_field_to_properties(streams[stream_name], shared_field)
        _add_field_to_metadata(streams[stream_name], shared_field)
    return streams


# this function iterates over all sources and creates a table per mdm_type
# an mdm_type may come from multiple sources
def _convert_to_singer_streams(streams):
    # streams = {}
    # for source in client.yield_sources():
    #     _get_schema_from_source(streams, source)
    singer_streams = {}
    for stream_name, data in streams.items():
        schema = {
            'type': 'object',
            'additionalProperties': False,
            'properties': data['properties']
        }
        metadata = []
        for prop, meta in data['metadata'].items():
            breadcrumb = [] if prop == '__table__' else ['properties', prop]
            metadata.append({'breadcrumb': breadcrumb, 'metadata': meta})
        singer_streams[stream_name] = {'schema': schema, 'metadata': metadata}
    return singer_streams


def discover(client):
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        pk = get_pk(stream_name)
        metadata = field_metadata[stream_name]
        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=pk,
            schema=schema,
            metadata=metadata
        ))

    for fn in [_records_streams, _partner_records_streams]:
        singer_streams = _convert_to_singer_streams(fn(client))
        for stream_name, data in singer_streams.items():
            schema = Schema.from_dict(data['schema'])
            metadata = data['metadata']
            catalog.streams.append(CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=PRIMARY_KEYS[stream_name],
                schema=schema,
                metadata=metadata
            ))

    return catalog
