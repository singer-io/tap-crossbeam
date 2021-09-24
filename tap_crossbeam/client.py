import backoff
import requests
import singer
from singer import metrics
from ratelimit import limits, sleep_and_retry, RateLimitException
from requests.exceptions import Timeout

LOGGER = singer.get_logger()

class Server5xxError(Exception):
    pass
# pylint: disable=too-many-instance-attributes
class CrossbeamClient():
    DEFAULT_BASE_URL = 'https://api.crossbeam.com'
    DEFAULT_AUTH_BASE_URL = 'https://auth.crossbeam.com'

    def __init__(self, config):
        self.__user_agent = config.get('user_agent')
        self.__organization_uuid = config.get('organization_uuid')
        self.__client_id = config.get('client_id')
        self.__client_secret = config.get('client_secret')
        self.__refresh_token = config.get('refresh_token')
        self.__base_url = config.get('base_url', self.DEFAULT_BASE_URL)
        self.__auth_base_url = config.get('auth_base_url', self.DEFAULT_AUTH_BASE_URL)
        self.__verify_ssl_certs = config.get('verify_ssl_certs', True)
        self.__default_headers = {'X-Requested-With': 'stitch'}

        self.__session = requests.Session()
        self.__access_token = None

    def __enter__(self):
        return self

    def __exit__(self, exit_type, value, traceback):
        self.__session.close()

    def refresh_access_token(self):
        data = self.request(
            'POST',
            url=f'{self.__auth_base_url}/oauth/token',
            data={
                'client_id': self.__client_id,
                'client_secret': self.__client_secret,
                'refresh_token': self.__refresh_token,
                'grant_type': 'refresh_token'
            },
            skip_auth=True)

        self.__access_token = data['access_token']

    @backoff.on_exception(backoff.expo,
                          (Server5xxError,
                           RateLimitException,
                           requests.exceptions.ConnectionError,
                           Timeout),
                          max_tries=5,
                          factor=3)
    @sleep_and_retry
    @limits(calls=300, period=60)
    def request(self,
                method,
                path=None,
                url=None,
                skip_auth=False,
                **kwargs):
        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        for header, value in self.__default_headers.items():
            kwargs['headers'][header] = value

        if not skip_auth:
            if not self.__access_token:
                self.refresh_access_token()
            kwargs['headers']['Authorization'] = f'Bearer {self.__access_token}'
            kwargs['headers']['Xbeam-Organization'] = self.__organization_uuid

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        kwargs['verify'] = self.__verify_ssl_certs

        if not url:
            url = self.__base_url + path

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        response.raise_for_status()

        return response.json()

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    def _yield_helper(self, path, endpoint, *, items_key='items'):
        next_href = None
        while path or next_href:
            LOGGER.debug('%s - Fetching %s', endpoint, path or next_href)
            data = self.get(path, url=next_href, endpoint=endpoint)
            yield from data[items_key]
            path = None
            next_href = data.get('pagination', {}).get('next_href')

    def yield_sources(self):
        yield from self._yield_helper('/v0.1/sources', 'sources')

    def yield_receiving_data_shares(self):
        yield from self._yield_helper('/v0.1/data-shares', 'data_shares',
                                      items_key='receiving_data_shares')

    def yield_partner_shared_fields(self):
        # Yep, for more specifics on that endpoint, for /partner-records
        # fields, pull from /v0.1/data-shares , receiving_data_shares key.
        # Within that, you will see a list of items, each of which has a
        # shared_fields key. In there, pull all the display_name fields by
        # mdm_type (same rules as for /records, lead and account map to
        # top_level, user maps to owner, anything can be ignored). Do note that
        # you will see a lot of duplicates here, so you’ll want to distinct
        # these all together. This will be in prod in the next hour.
        for data_share in self.yield_receiving_data_shares():
            yield from data_share['shared_fields']

    def yield_records(self):
        yield from self._yield_helper('/v0.1/records?limit=1000', 'records')

    def yield_partner_records(self):
        yield from self._yield_helper('/v0.1/partner-records?limit=1000', 'partner_records')

    def yield_partners(self):
        yield from self._yield_helper('/v0.1/partners', 'partners', items_key='partner_orgs')
