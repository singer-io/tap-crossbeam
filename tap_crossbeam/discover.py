import os
import json

from singer.catalog import Catalog, CatalogEntry, Schema

from tap_crossbeam.endpoints import ENDPOINTS_CONFIG

SCHEMAS = {}
FIELD_METADATA = {}


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
    global SCHEMAS, FIELD_METADATA

    if SCHEMAS:
        return SCHEMAS, FIELD_METADATA

    schemas_path = get_abs_path('schemas')

    file_names = [f for f in os.listdir(schemas_path)
                  if os.path.isfile(os.path.join(schemas_path, f))]

    for file_name in file_names:
        stream_name = file_name[:-5]
        with open(os.path.join(schemas_path, file_name)) as data_file:
            schema = json.load(data_file)

        SCHEMAS[stream_name] = schema

        pk = get_pk(stream_name)

        metadata = []
        for prop, _ in schema['properties'].items():
            if prop in pk:
                inclusion = 'automatic'
            else:
                inclusion = 'available'
            metadata.append({
                'metadata': {
                    'inclusion': inclusion
                },
                'breadcrumb': ['properties', prop]
            })
        FIELD_METADATA[stream_name] = metadata

    return SCHEMAS, FIELD_METADATA


def _field_jschema_type(field):
    cb_type = field['data_type']
    cb_pg_type = field['pg_data_type']
    if cb_type == 'datetime':
        return ('string', 'date-time')
    if cb_type == 'number':
        if cb_pg_type == 'bigint':
            return ('integer', None)
        return ('number', None)
    if cb_type == 'boolean':
        return ('boolean', None)
    return ('string', None)


def _add_fields_to_properties(properties, source):
    for field in source['fields']:
        column_name = field['display_name']
        json_type, json_format = _field_jschema_type(field)
        if column_name in properties:
            if json_type not in properties[column_name]['type']:
                properties[column_name]['type'].append(json_type)
        else:
            json_schema = {'type': ['null', json_type]}
            if json_format:
                json_schema['format'] = json_format
            properties[column_name] = json_schema


def _add_fields_to_metadata(metadata, source):
    for field in source['fields']:
        column_name = field['display_name']
        if column_name not in metadata:
            metadata[column_name] = {'inclusion': 'available'}


def get_schema_from_source(streams, source):
    stream_name = source['mdm_type']
    streams[stream_name] = streams.get(stream_name) or {
        'properties': {
            '_crossbeam_id': {'type': ['string']},
            '_record_id': {'type': ['string']},
            '_updated_at': {'type': ['null', 'string'], 'format': 'date-time'},
        },
        'metadata': {
            '__table__': {
                'tap-crossbeam.is_partner_data': True,
                'tap-crossbeam.schema': source['schema'],
                'tap-crossbeam.table': source['table'],
                'tap-crossbeam.mdm_type': source['mdm_type'],
            }
        },
    }
    _add_fields_to_properties(streams[stream_name]['properties'], source)
    _add_fields_to_metadata(streams[stream_name]['metadata'], source)


# this function iterates over all sources and creates a table per mdm_type
# an mdm_type may come from multiple sources
def get_partner_data_schemas(client):
    streams = {}

    path = '/v0.1/sources'
    next_href = None
    while path or next_href:
        data = client.get(path, url=next_href, endpoint='sources')
        for source in data['items']:
            get_schema_from_source(streams, source)

        path = None
        next_href = data.get('pagination', {}).get('next_href')

    # turn streams from a compact representation to singer rep
    singer_streams = {}
    for stream_name, data in streams.items():
        schema = {
            'type': 'object',
            'additionalProperties': False,
            'properties': data['properties']
        }
        metadata = []
        for prop, meta in data['metadata'].items():
            if prop == '__table__':
                breadcrumb = []
            else:
                breadcrumb = ['properties', prop]
            metadata.append(
                {
                    'breadcrumb': breadcrumb,
                    'metadata': meta
                }
            )
        singer_streams[stream_name] = {
            'schema': schema,
            'metadata': metadata
        }
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

    partner_singer_streams = get_partner_data_schemas(client)
    for stream_name, data in partner_singer_streams.items():
        schema = Schema.from_dict(data['schema'])
        metadata = data['metadata']

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=['_crossbeam_id'],
            schema=schema,
            metadata=metadata
        ))

    return catalog
