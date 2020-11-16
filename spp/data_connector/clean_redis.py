# For some reasons the expired keys are not deleted by the redis database
# this script removes the expired keys

from microquake.db.connectors import connect_redis
r = connect_redis()

keys = []
expired_keys = []
for key in r.scan_iter():
    print(f'{key} {r.ttl(key)}')
    if r.ttl(key) == -1:
        r.delete(key)
