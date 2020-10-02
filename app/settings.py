from pydantic import (
    BaseSettings,
    PyObject,
)


class Settings(BaseSettings):
    backend: PyObject = 'app.backends.dynamodb.Backend'
    initialize: bool = False

    class Config:
        env_prefix = 'cascade_'


settings = Settings()
backend = settings.backend()
