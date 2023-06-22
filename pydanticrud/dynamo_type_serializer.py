import decimal
from typing import Any, Mapping, Union

NoneType = type(None)


class DynamoTypeSerializer:
    def __init__(self):
        self._methods = {
            bool: serialize_bool,
            decimal.Decimal: serialize_number,
            dict: self.serialize_mapping,
            int: serialize_number,
            list: self.serialize_list,
            Mapping: self.serialize_mapping,
            NoneType: serialize_none,
            str: serialize_str,
        }

    def serialize_item(self, item) -> dict:
        return {k: self.serialize(v) for k, v in item.items()}

    def serialize_mapping(self, value: Mapping) -> dict:
        return {"M": {k: self.serialize(v) for k, v in value.items()}}

    def serialize_list(self, value: Union[list, tuple]) -> dict:
        return {"L": [self.serialize(item) for item in value]}

    def serialize(self, value: Any):
        value_type = type(value)
        try:
            route = self._methods[value_type]
        except KeyError:
            for type_route, route in self._methods.items():
                if issubclass(value_type, type_route):
                    self._methods[value_type] = route
                    return route(value)
        else:
            return route(value)
        raise TypeError(f"The value {value} is not Dynamodb serializable type.")


def serialize_bool(value: bool) -> dict:
    return {"BOOL": value}


def serialize_number(value: Union[int, float, decimal.Decimal]) -> dict:
    return {"N": str(value)}


def serialize_none(value: None) -> dict:
    return {"NULL": True}


def serialize_str(value: str) -> dict:
    return {"S": value}
