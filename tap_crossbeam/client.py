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
    DEFAULT_BASE_URL = 'https://api.getcrossbeam.com'

    def __init__(self, config):
        self.__user_agent = config.get('user_agent')
        self.__org_id = config.get('org_id')
        self.__access_token = config.get('access_token')
        self.__base_url = config.get('base_url', self.DEFAULT_BASE_URL)

        self.__session = requests.Session()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__session.close()

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
                **kwargs):
        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        kwargs['headers']['Authorization'] = 'Bearer {}'.format(self.__access_token)
        kwargs['headers']['Xbeam-Organization'] = self.__org_id

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
