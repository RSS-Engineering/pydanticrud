# PydantiCRUD

Supercharge your Pydantic models with CRUD methods and a pluggable backend

pydanticrud let's you add a few details to your pydantic models to get basic
CRUD methods automatically built into your models for the backend of your
choice.

## Usage

```python
from pydanticrud import BaseModel, DynamoDbBackend

class User(BaseModel):
    id: int
    name: str

    class Config:
        title = 'User'
        backend = DynamoDbBackend
        hash_key = 'id'
```

First, use the `BaseModel` from `pydanticrud` instead of `pydantic`.

Next add your backend to your model's `Config` class. PydantiCRUD is geared
toward DynamoDB but provides SQLite for lighter-weight usage. You can provide
your own if you like.

Finally, add appropriate members to the `Config` class for the chosen backend.

## Methods

### Model Methods

`get(id)` - return and instance of the Model from the backend indexed by `id`

`exists()` - detect if the model (table) exists in the backend

`initialize()` - setup the model (table) in the backend

`delete(id)` - delete the record from backend

`query(rule)` - return a list of records that satify the rule. Rules are
defined by [rule-engine](https://zerosteiner.github.io/rule-engine/) for
querying or filtering the backend.

NOTE: Rule complexity is limited by the querying capabilities of the backend.

### Instance Methods

`save()` - store the Model instance to the backend

### DynamoDB

`query(query_expr: Optional[Rule], filter_expr: Optional[Rule])` - Providing a
  `query_expr` parameter will try to apply the keys of the expression to an
  existing index. Providing a `filter_expr` parameter will filter the results of
  a passed `query_expr` or run a dynamodb `scan` if no `query_expr` is passed.
  An empty call to `query()` will return the scan results (and be resource
  intensive).

## Backend Configuration Members

`hash_key` - the name of the key field for the backend table

### DynamoDB

`hash_key` - the name of the [partition key](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html#HowItWorks.CoreComponents.PrimaryKey) field for the backend table.

`range_key` - (optional) the name of the [sort key](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html#HowItWorks.CoreComponents.PrimaryKey) field for the backend table for composite key tables.

`region` - (optional) specify the region to access dynamodb in.

`endpoint` - (optional) specify an endpoint to use a local or non-AWS implementation of DynamoDB

`local_indexes` - (optional) specify a mapping of index-name to tuple(partition_key).

`global_indexes` - (optional) specify a mapping of index-name to tuple(partition_key).

### SQLite (Python 3.7+)

`database` - the filename of the database file for SQLite to use

## Roadmap

There is plenty of room for improvement to PydantiCRUD.

- Backend feature support not being consistent is the most egregious flaw that
can be incrementally improved.
- SQLite JSON1 extension support for more powerful queries.
- Add more backends

  - REST API
  - Redis
  - Postgres/MySQL

## Tools

### Testing

You can run unittests with `poetry run pytest`

### Formatting

You can format the code by running:

`for d in pydanticrud tests; do poetry run black $d; done`
