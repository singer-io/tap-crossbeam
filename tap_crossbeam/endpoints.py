
ENDPOINTS_CONFIG = {
    'partner_populations': {
        'path': '/v0.1/partner-populations',
        'pk': ['id']
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
        'children': {
            'reports_data': {
                'path': '/v0.1/reports/{report_id}/data',
                'pk': ['master_id'],
                'params': {
                    'limit': 100
                }
            }
        }
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
