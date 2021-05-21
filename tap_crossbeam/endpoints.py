
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
