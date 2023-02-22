import json
import requests
import redis
import time
from datetime import datetime
URL = 'http://localhost:8090/api/v1/property/types/?limit=2'  # not a real url obviously
red = redis.StrictRedis(
    'localhost',
    6379,
    db=9,
    password='eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81',
    charset="utf-8",
    decode_responses=True
)


def stream():
    s = requests.Session()
    r = s.get(URL, stream=True)
    r = r.json()
    print(datetime.now().timestamp())
    red.publish('LOGS', json.dumps(r))

def stream2():
    data = {
        'log_type': 'activity_logs_v1',
        'data': {
            'subject': 1,
            'action': 'Added ABC',
            'version': 1,
            'url': 'http://loalhost:8090/home',
            'ip_address': '123.33.55.6',
            'log_id': 'LOG-353465-435634',
            'user': {
                'identity': '325434543543543',
                'type_id': 534,
                'name': 'erretr rry',
            },
            'parent': 545,

        }
    }
    red.publish('LOGS', json.dumps(data))

if __name__ == "__main__":
    while True:
        time.sleep(4)
        stream2()

