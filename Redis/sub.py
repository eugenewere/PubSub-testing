import redis
from datetime import datetime
red = redis.StrictRedis(
    'localhost',
    6379,
    db=11,
    password='eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81',
    charset="utf-8",
    decode_responses=True
)

def user_counter():
    sub = red.pubsub()
    sub.subscribe('types')
    for message in sub.listen():
        print(datetime.now().timestamp())
        if message is not None and isinstance(message, dict):
            print(message)
            username = message.get('data')
            red.zincrby('username_scoreboard', 1, username)


while True:
    user_counter()
