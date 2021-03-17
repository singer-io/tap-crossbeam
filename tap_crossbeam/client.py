import json
from datetime import datetime, timedelta

import backoff
import requests
import singer
from singer import metrics
from ratelimit import limits, sleep_and_retry, RateLimitException
from requests.exceptions import ConnectionError, Timeout

LOGGER = singer.get_logger()

class Server5xxError(Exception):
    pass

class CrossbeamClient(object):
    DEFAULT_BASE_URL = 'https://api.crossbeam.com'
    DEFAULT_AUTH_BASE_URL = 'https://auth.crossbeam.com'

    def __init__(self, config, config_path):
        self.__user_agent = config.get('user_agent')
        self.__organization_uuid = config.get('organization_uuid')
        self.__client_id = config.get('client_id')
        self.__client_secret = config.get('client_secret')
        self.__refresh_token = config.get('refresh_token')
        self.__base_url = config.get('base_url', self.DEFAULT_BASE_URL)
        self.__auth_base_url = config.get('auth_base_url', self.DEFAULT_AUTH_BASE_URL)
        self.__config_path = config_path

        self.__session = requests.Session()
        self.__access_token = None
        self.__expires_at = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__session.close()

    def refresh_access_token(self):
        data = self.request(
            'POST',
            url='{}/oauth/token'.format(self.__auth_base_url),
            data={
                'client_id': self.__client_id,
                'client_secret': self.__client_secret,
                'refresh_token': self.__refresh_token,
                'grant_type': 'refresh_token'
            },
            skip_auth=True)

        self.__access_token = data['access_token']

        self.__user_expires_at = datetime.utcnow() + \
            timedelta(seconds=data['expires_in'] - 10) # pad by 10 seconds for clock drift

        ## Update refresh token in config file
        with open(self.__config_path) as file:
            config = json.load(file)
        config['refresh_token'] = self.__refresh_token
        with open(self.__config_path, 'w') as file:
            json.dump(config, file, indent=2)

    @backoff.on_exception(backoff.expo,
                          (Server5xxError,
                           RateLimitException,
                           ConnectionError,
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

        if not skip_auth:
            if not self.__access_token:
                self.refresh_access_token()
            kwargs['headers']['Authorization'] = 'Bearer {}'.format(self.__access_token)
            kwargs['headers']['Xbeam-Organization'] = self.__organization_uuid

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

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
