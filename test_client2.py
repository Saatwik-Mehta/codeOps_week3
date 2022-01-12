import requests
import json


class TestClient():
    def __init__(self):
        self.test_client = requests.request

    def send(self, url, method='GET', data=None, headers={}):
        # append the autnentication headers to all requests
        headers = headers.copy()
        headers['Content-Type'] = 'application/json'
        headers['Accept'] = 'application/json'

        # convert JSON data to a string
        if data:
            data = json.dumps(data)
        # send request to the test client and return the response
        rv = self.test_client(method, url, data=data, headers=headers)
        return rv, rv.json()

    def get(self, url, headers={}):
        return self.send(url, 'GET', headers=headers)

    def post(self, url, data, headers={}):
        return self.send(url, 'POST', data, headers=headers)

    def put(self, url, data, headers={}):
        return self.send(url, 'PUT', data, headers=headers)

    def delete(self, url, headers={}):
        return self.send(url, 'DELETE', headers=headers)


if __name__ == '__main__':
    client = TestClient()
    resp, data = client.get('https://collectionapi.metmuseum.org/public/collection/v1/objects/1')
    print(resp)
    print(data)






