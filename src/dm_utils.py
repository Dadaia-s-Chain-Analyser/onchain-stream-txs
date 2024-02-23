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

