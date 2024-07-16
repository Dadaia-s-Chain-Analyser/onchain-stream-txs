import json
import os
import time
from redis import Redis



if __name__ == "__main__":

  REDIS_HOST = os.getenv("REDIS_HOST")
  REDIS_PORT = os.getenv("REDIS_PORT")
  REDIS_PASS = os.getenv("REDIS_PASS")
  FREQ = float(os.getenv("FREQUENCY"))


  REDIS_CLIENT = Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS)

  while 1:
    keys = [key.decode('utf-8') for key in REDIS_CLIENT.keys() if key.decode('utf-8') != "api_keys_semaphore"]
    total = {key: REDIS_CLIENT.hgetall(key) for key in keys}
    for key in total:
      total[key] = {k.decode('utf-8'): v.decode('utf-8') for k, v in total[key].items()}
    REDIS_CLIENT.set("api_keys_semaphore", json.dumps(total))
    time.sleep(FREQ)