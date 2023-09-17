# migraine

Implementation of automated semantic versioning-based Mongo migration flow for Python

## Installation

The recommended way to install migraine is with [Poetry][poetry] package manager:

```sh
poetry add git+https://github.com/hacksparr0w/migraine
```

## Usage

Consider the following project structure:

```
.
├── migrations
│   ├── 0.1.0.py
│   ├── 0.2.0.py
│   └── 0.3.0.py
├── app.py
└── pyproject.toml
```

Note that your `pyproject.toml` must have a valid `version` string in the
`tool.poetry` section for migraine to be able to determine the current version
of your project.

Next, you need to call the `migraine.migrate` function on the start of your
application:

```py
# app.py

import asyncio
import logging

import migraine

from motor.motor_asyncio import AsyncIOMotorClient


async def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%m-%d %H:%M"
    )

    client = AsyncIOMotorClient("mongodb://mongo:27017/?directConnection=true")
 
    await migraine.migrate(client)


if __name__ == "__main__":
    asyncio.run(main())
```

Migraine will automatically detect the current schema version of your database
and apply all neccessary migration scripts from the `migrations` directory.


The migration scripts themselves are just Python files with the following
interface:

```py
# 0.1.0.py
from motor.motor_asyncio import AsyncIOMotorClientSession


async def __apply__(session: AsyncIOMotorClientSession) -> None:
    database = session.client.get_database("app")
    collection = database.get_collection("users")

    initial_user = {
        "name": "John Doe",
        "age": 42
    }

    await collection.insert_one(initial_user, session=session)


async def __revert__(session: AsyncIOMotorClientSession) -> None:
    database = session.client.get_database("app")
    collection = database.get_collection("users")

    await collection.delete_many({}, session=session)
```

The `__apply__` function is called when the migration is applied, and the
`__revert__` function is called when the migration is reverted.


[poetry]: https://python-poetry.org/
