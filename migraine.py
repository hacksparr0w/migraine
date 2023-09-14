from __future__ import annotations

import importlib.util
import inspect
import sys
import tomllib

from datetime import datetime
from enum import Enum
from functools import cmp_to_key
from pathlib import Path
from types import ModuleType

import semver

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase
)

from pydantic import BaseModel


_PROJECT_METADATA_FILE_NAME = "pyproject.toml"
_PROJECT_MIGRATION_DIRECTORY_NAME = "migrations"

_MIGRATION_DATABASE_NAME = "__migraine__"
_MIGRATION_COLLECTION_NAME = "migrations"


class _MigrationDirection(Enum):
    FORWARD = "FORWARD"
    BACKWARD = "BACKWARD"


_MIGRATION_MODULE_API = {
    _MigrationDirection.FORWARD: "__apply__",
    _MigrationDirection.BACKWARD: "__revert__"
}

_MIGRATION_MOFULE_NAME_FORMAT = "migration_{version}"


class MigrationError(Exception):
    pass


class ProjectInspectionError(Exception):
    pass


class _Migration(BaseModel):
    version: str
    application_datetime: datetime

    @classmethod
    def of(cls, version: str) -> _Migration:
        return cls(version=version, application_datetime=datetime.now())


def _get_calling_module() -> ModuleType:
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])

    return module


def _load_module(name: str, file: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, str(file))

    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)

    return module


def _to_versioned(file: Path) -> tuple[str, Path]:
    version = file.stem

    try:
        semver.Version.parse(version)
    except ValueError:
        raise ProjectInspectionError(
            f"Migration file '{file}' is not named as a valid semantic"
             " version"
        )

    return version, file


def _to_module_name(version: str) -> str:
    version = version \
        .replace(".", "_") \
        .replace("-", "_") \
        .replace("+", "_")

    return _MIGRATION_MOFULE_NAME_FORMAT.format(version=version)


def _find_project_metadata_file() -> Path:
    directory = Path(_get_calling_module().__file__).parent

    while directory != Path("/"):
        metadata_file = directory / _PROJECT_METADATA_FILE_NAME

        if metadata_file.exists():
            return metadata_file

        directory = directory.parent

    raise ProjectInspectionError(
        f"Could not find '{_PROJECT_METADATA_FILE_NAME}' file"
    )


def _load_project_metadata(file: Path) -> dict[str, str]:
    with file.open(encoding="utf-8") as stream:
        return tomllib.load(stream)


def _get_project_version(metadata: dict[str, str]) -> str:
    return metadata["tool"]["poetry"]["version"]


def _list_migration_script_files(
    project_root_directory: Path
) -> list[tuple[str, Path]]:
    migration_directory = (
        project_root_directory / _PROJECT_MIGRATION_DIRECTORY_NAME
    )

    candidate_files = migration_directory.glob("*.py")
    migration_files = map(_to_versioned, candidate_files)
    compare = lambda a, b: semver.compare(a[0], b[0])

    return sorted(migration_files, key=cmp_to_key(compare))


def _database(client: AsyncIOMotorClient) -> AsyncIOMotorDatabase:
    return client.get_database(_MIGRATION_DATABASE_NAME)


def _collection(db: AsyncIOMotorDatabase) -> AsyncIOMotorCollection:
    return db.get_collection(_MIGRATION_COLLECTION_NAME)


async def _find_last_migration(
    session: AsyncIOMotorClientSession,
    collection: AsyncIOMotorCollection
) -> _Migration | None:
    migrations = await collection \
        .find(session=session) \
        .sort("application_datetime", -1) \
        .limit(1) \
        .to_list(1)

    if len(migrations) == 0:
        return None

    return _Migration(**migrations[0])


async def _insert_migration(
    session: AsyncIOMotorClientSession,
    collection: AsyncIOMotorCollection,
    migration: _Migration
) -> None:
    await collection.insert_one(migration.dict(), session=session)


async def _insert_migration_of(
    session: AsyncIOMotorClientSession,
    collection: AsyncIOMotorCollection,
    version: str
) -> None:
    await _insert_migration(session, collection, _Migration.of(version))


async def _run_migration_script(
    session: AsyncIOMotorClientSession,
    version: str,
    file: Path,
    direction: _MigrationDirection
) -> None:
    module = _load_module(_to_module_name(version), file)
    function = getattr(module, _MIGRATION_MODULE_API[direction])

    await function(session)


async def migrate(client: AsyncIOMotorClient) -> None:
    project_metadata_file = _find_project_metadata_file()
    project_root_directory = project_metadata_file.parent
    project_metadata = _load_project_metadata(project_metadata_file)
    current_project_version = _get_project_version(project_metadata)

    async with client.start_session() as session:
        async with session.start_transaction():
            database = _database(client)
            collection = _collection(database)

            last_migration = await _find_last_migration(session, collection)

            if last_migration is None:
                await _insert_migration_of(
                    session,
                    collection,
                    current_project_version
                )

                return

            current_database_verstion = last_migration.version
            version_comparison = semver.compare(
                current_project_version,
                current_database_verstion
            )

            if version_comparison == 0:
                return

            migration_script_files = _list_migration_script_files(
                project_root_directory
            )

            migration_direction = None

            if version_comparison == -1:
                migration_direction = _MigrationDirection.BACKWARD
                migration_script_files = filter(
                    lambda x: (
                        semver.compare(current_project_version, x[0]) == -1
                    ),
                    migration_script_files
                )

                migration_script_files = map(
                    lambda x: x[1], migration_script_files
                )

                migration_script_files = reversed(migration_script_files)
            elif version_comparison == 1:
                migration_direction = _MigrationDirection.FORWARD
                migration_script_files = filter(
                    lambda x: semver.compare(current_project_version, x[0]) == 1,
                    migration_script_files
                )

                migration_script_files = map(lambda x: x[1], migration_script_files)
            else:
                raise RuntimeError("Unexpected version comparison result")

            for file in migration_script_files:
                await _run_migration_script(
                    database,
                    current_project_version,
                    file,
                    migration_direction
                )

            await _insert_migration_of(database, current_project_version)
