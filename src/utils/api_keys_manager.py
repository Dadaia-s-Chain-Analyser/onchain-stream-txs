from typing import List
from datetime import datetime as dt

from cassandra.cluster import Session
from redis import Redis
from logging import Logger


class APIKeysManager:

  def __init__(self, logger: Logger, pid: str, scylla_session: Session, scylla_table: str, redis_client: Redis):
    self.logger = logger
    self.pid = pid
    self.scylla_session = scylla_session
    self.scylla_table = scylla_table
    self.redis_client = redis_client


  def get_api_keys_stored_in_cassandra(self) -> List[str]:
    try:
      rows = self.scylla_session.execute(f'SELECT name FROM {self.scylla_table}')
      return [row.name for row in rows]
    except Exception as e:
      self.logger.error(f"Error while reading from ScyllaDB: {e}")
      return []
    

  def insert_api_keys_into_scylla(self, api_keys: List[str]) -> None:
    query_base = f"INSERT INTO {self.scylla_table} "
    query_base += "(name, start, end, num_req_1d, last_req) VALUES"
    TEMPL_QUERY = lambda apk, timestamp: f"{query_base} ('{apk}', null, null, 0, '{timestamp}')"
    for api_key in api_keys:
      query = TEMPL_QUERY(api_key, dt.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
      self.scylla_session.execute(query)
    return


  def populate_scylla_with_missing_api_keys(self, api_keys: List[str]) -> None:
    stored_api_keys = self.get_api_keys_stored_in_cassandra()
    missing_stored_keys = list(set(api_keys) - set(stored_api_keys))
    self.insert_api_keys_into_scylla(missing_stored_keys)
    return


  def get_data_from_scylla(self) -> List[str]:
    rows = self.scylla_session.execute(f'SELECT * FROM {self.scylla_table}')
    all_api_keys_data = map(lambda x: (x.name, x.num_req_1d, x.end), rows)
    infura_api_keys_data = filter(lambda x: "infura" in x[0], all_api_keys_data)
    api_keys_sorted = sorted(infura_api_keys_data, key=lambda x: (x[2], x[1]))
    return [api_key[0] for api_key in api_keys_sorted]


  def elect_new_api_key(self) -> str:
    api_keys_scylla = self.get_data_from_scylla()
    api_keys_used = self.redis_client.keys()
    for api_key in api_keys_scylla:
      if api_key not in api_keys_used:
        self.logger.info(f"API KEY ELECTED: {api_key}")
        return api_key
      
                     
  def free_api_keys(self, free_timeout: float = 15) -> None:
    api_keys_data_cached = {api_key: self.redis_client.hgetall(api_key) for api_key in self.redis_client.keys()}
    api_keys_to_free = [api_key for api_key, value in api_keys_data_cached.items() if dt.now().timestamp() - float(value["last_update"]) > free_timeout]
    for api_key in api_keys_to_free:
      self.redis_client.delete(api_key)
      self.logger.info(f"API KEY FREE: {api_key}")


  def check_api_key_request(self, api_key: str) -> None:
    self.redis_client.hset(api_key, mapping={"process": self.pid, "last_update":  dt.now().timestamp()})


  def decompress_api_key_names(self, api_keys_compacted: str) -> List[str]:
    """Receives a string -> Ex. infura-1-3
       Returns a list of strings -> Ex. [infura-1, infura-2, infura-3]
    """
    interval = [int(i) for i in api_keys_compacted.split("-")[-2:]]
    name_secret = "-".join(api_keys_compacted.split("-")[:-2])
    api_keys = [f"{name_secret}-{i}" for i in range(interval[0], interval[1] + 1)]
    return api_keys
  