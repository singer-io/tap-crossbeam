
ENDPOINTS_CONFIG = {
    'partner_populations': {
        'path': '/v0.1/partner-populations',
        'pk': ['id']
    },
    # partner_orgs? proposals? proposals_received?
    'partners': {
        'path': '/v0.1/partners',
        'pk': ['id']
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
    }
}
