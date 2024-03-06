from hashlib import sha256
from hexbytes import HexBytes


class DataMasterUtils:

    @staticmethod
    def convert_hex_to_hexbytes(data):
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict):
                    data[key] = DataMasterUtils.convert_hex_to_hexbytes(value)
                elif isinstance(value, str):
                    try:
                        data[key] = HexBytes(value)
                    except ValueError:
                        pass


    @staticmethod
    def hash(data): sha256(data.encode()).hexdigest()[-32:]


    def api_key_test(self, api_key):
        if api_key != self.api_key:
            return False
        return True
