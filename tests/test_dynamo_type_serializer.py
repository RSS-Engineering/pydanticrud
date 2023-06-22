from decimal import Decimal

from pydanticrud.backends.dynamodb import DynamoSerializer

serializer = DynamoSerializer(schema={})
def test_int():
    assert serializer.serialize(0) == {'N': '0'}
    assert serializer.serialize(34) == {'N': '34'}


def test_decimal():
    assert serializer.serialize(Decimal('1.2345')) == {'N': '1.2345'}
    assert serializer.serialize(Decimal('0.00')) == {'N': '0.00'}

def test_str():
    assert serializer.serialize('') == {'S': ''}
    assert serializer.serialize('hello') == {'S': 'hello'}
    assert serializer.serialize('hello world') == {'S': 'hello world'}

def test_none():
    return serializer.serialize(None) == {'NULL': True}

def test_bools():
    assert serializer.serialize(True) == {'BOOL': True}
    assert serializer.serialize(False) == {'BOOL': False}


def test_list():
    assert serializer.serialize([1, 'abc', None, None, True]) == {
        'L': [{'N': '1'}, {'S': 'abc'}, {'NULL': True}, {'NULL': True},
              {'BOOL': True}]
    }

def test_maps():
    assert serializer.serialize({
        'hello': 'world',
        'another': 123,
        'loop': {'Working': True}
    }) == {
        'M': {
            'hello': {'S': 'world'},
            'another': {'N': '123'},
            'loop': {'M': {'Working': {'BOOL': True}}}
        }
    }

def test_item():
    assert serializer.serialize_item({
        'Hello': 'World',
        'another': 123,
        'loop': {'Working': True}
    }) == {
        'Hello': {'S': 'World'},
        'another': {'N': '123'},
        'loop': {'M': {'Working': {'BOOL': True}}}
    }
