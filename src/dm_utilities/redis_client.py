import json
import redis


class RedisClient:
    def __init__(self, host: str, port: int):
        self.redis = redis.Redis(host=host, port=port, decode_responses=True)
        self.redis.keys()

    def insert_key_obj(
        self, key: str, value: object, overwrite: bool = True
    ) -> None:
        """
        Insere uma chave no redis
        :param key: chave a ser inserida
        :param value: valor da chave
        :param overwrite: se True, sobrescreve a chave se ela já existir
        :return: None
        """
        key_exists = self.redis.exists(key)
        if (key_exists and overwrite) or not key_exists:
            self.redis.set(key, json.dumps(value))
        else:
            print(f'Key {key} already exists')

    def get_key_obj(self, key, default=[]) -> object:
        data = self.redis.get(key)
        if data is None:
            return default
        return json.loads(data)

    def delete_key(self, key) -> None:
        self.redis.delete(key)

    def clear_keys(self) -> None:
        self.redis.flushall()

    def list_keys(self) -> list:
        return self.redis.keys()
