import aerospike
from aerospike import exception as e
import time


def ensure_dropped_index(client, namespace, index_name):
    start = time.time()
    retries = 0
    while retries < 10:
        try:
            client.index_remove(namespace, index_name)
        except e.IndexNotFound:
            return
        time.sleep(.5)
        retries += 1
