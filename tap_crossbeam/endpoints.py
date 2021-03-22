
ENDPOINTS_CONFIG = {
    'partner_populations': {
        'path': '/v0.1/partner-populations',
        'pk': ['id']
    },
    'partner_records': {
        'path': '/v0.1/partner-records',
        'pk': ['_partner_organization_id', '_partner_record_id'],
    },
    'partners': {
        'path': '/v0.1/partners',
        'pk': ['id'],
        'data_key': 'partner_orgs'
    },
    'populations': {
        'path': '/v0.1/populations',
        'pk': ['id']
    },
    'reports': {
        'path': '/v0.2/reports',
        'pk': ['id'],
        'provides': {
            'report_id': ['id']
        },
    },
    'threads': {
        'path': '/v0.1/threads',
        'pk': ['id'],
        'provides': {
            'thread_id': ['id']
        },
        'children': {
            'thread_timelines': {
                'path': '/v0.1/threads/{thread_id}/timeline',
                'pk': ['id']
            }
        }
    }
}
