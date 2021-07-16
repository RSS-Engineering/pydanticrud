# PydantiCRUD
Supercharge your Pydantic models with CRUD methods and a pluggable backend

pydanticrud let's you add a few details to your pydantic models to get basic 
CRUD methods automatically built into your models for the backend of your
choice.

## Usage

```python
from pydanticrud import BaseModel, SqliteBackend

class User(BaseModel):
    id: int
    name: str

    class Config:
        title = 'User'
        backend = SqliteBackend
        hash_key = 'id'
        database = ":memory:"
```

First, use the `BaseModel` from `pydanticrud` instead of `pydantic`.

Next add your backend to your model's `Config` class. PydantiCRUD provides SQLite
and DynamoDB backends. You can provide your own if you like.

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
For example: querying on a non-hash_key in dynamo will run a scan and be slow.

### Instance Methods

`save()` - store the Model instance to the backend

## Backend Configuration Members

`hash_key` - the name of the key field for the backend table

### DynamoDB

`region` - (optional) specify the region to access dynamodb in.

`endpoint` - specify an endpoint to use a local or non-AWS implementation of
DynamoDB

### SQLite

`database` - the filename of the database file for SQLite to use

## Roadmap

There is plenty of room for improvement to PydantiCRUD.

- Backend feature support not being consistent is the most egregious flaw that can be incrementally
improved.

- Add more backends

  - REST API
  - Redis
  - Postgres/MySQL
