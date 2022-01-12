import requests
import json
from urllib.parse import urlsplit, urlunsplit


class TestClient:
    def __init__(self):
        self.test_client = requests.request

    def send(self, url, method='GET', data=None, headers={}):
        # for testing, URLs just need to have the path and query string
        url_parsed = urlsplit(url)
        url = urlunsplit(('', '', url_parsed.path, url_parsed.query,
                          url_parsed.fragment))

        # append the autnentication headers to all requests
        headers = headers.copy()
        headers['Content-Type'] = 'application/json'
        headers['Accept'] = 'application/json'

        # convert JSON data to a string
        if data:
            data = json.dumps(data)
        # send request to the test client and return the response
        rv = self.test_client(method=method, url=url, data=data, headers=headers)
        return rv, json.loads(rv.json())

    def get(self, url, headers={}):
        return self.send(url, 'GET', headers=headers)

    def post(self, url, data, headers={}):
        return self.send(url, 'POST', data, headers=headers)

    def put(self, url, data, headers={}):
        return self.send(url, 'PUT', data, headers=headers)

    def delete(self, url, headers={}):
        return self.send(url, 'DELETE', headers=headers)

