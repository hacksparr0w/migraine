from __future__ import annotations

import importlib.util
import inspect
import logging
import sys
import tomllib

from collections.abc import Callable, Sequence
from datetime import datetime
from enum import Enum
from functools import cmp_to_key
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable, Mapping, TypeVar

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase
)

from pydantic import BaseModel, ConfigDict, field_serializer, field_validator
from semver import Version


logger = logging.getLogger(__name__)


__all__ = [
    "MigrationError",
    "ProjectInspectionError",
    "migrate"
]


_INITIAL_DATABASE_VERSION = Version.parse("0.0.0")

_MIGRATION_DATABASE_NAME = "__migraine__"
_MIGRATION_COLLECTION_NAME = "migrations"

_PROJECT_METADATA_FILE_NAME = "pyproject.toml"
_PROJECT_MIGRATION_DIRECTORY_NAME = "migrations"


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
    model_config = ConfigDict(arbitrary_types_allowed=True)

    version: Version
    application_datetime: datetime

    @field_serializer("version")
    def serialize_version(self, version: Version) -> str:
        return str(version)

    @field_validator("version")
    @classmethod
    def validate_version(cls, version: str | Version) -> Version:
        if isinstance(version, Version):
            return version

        return Version.parse(version)

    @classmethod
    def of(cls, version: Version) -> _Migration:
        return cls(version=version, application_datetime=datetime.now())


_MigrationStrategy = tuple[_MigrationDirection, list[Version]]
_VersionedPath = tuple[Version, Path]


T = TypeVar("T")


def _fst(items: Sequence[T]) -> T:
    return items[0]


def _snd(items: Sequence[T]) -> T:
    return items[1]


def _find(predicate: Callable[[T], bool], iterable: Iterable[T]) -> T:
    return next(filter(predicate, iterable))


def _get_calling_module() -> ModuleType:
    frame_info = _snd(inspect.stack())
    module = inspect.getmodule(frame_info.frame)

    if module is None:
        raise ProjectInspectionError("Could not determine the calling module")

    return module


def _load_module(name: str, file: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, str(file))

    if not spec or not spec.loader:
        raise ProjectInspectionError(
            f"Could not load migration file '{file}' as a module"
        )

    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)

    return module


def _to_versioned(file: Path) -> _VersionedPath:
    try:
        version = Version.parse(file.stem)
    except ValueError:
        raise ProjectInspectionError(
            f"Migration file '{file}' is not named as a valid semantic"
             " version"
        )

    return version, file


def _to_module_name(version: Version) -> str:
    serialized_version = str(version) \
        .replace(".", "_") \
        .replace("-", "_") \
        .replace("+", "_")

    return _MIGRATION_MOFULE_NAME_FORMAT.format(version=serialized_version)


def _find_project_metadata_file() -> Path:
    calling_module_file = _get_calling_module().__file__

    if not calling_module_file:
        raise ProjectInspectionError(
            "Could not determine the calling module file"
        )

    directory = Path(calling_module_file).parent

    while directory != Path("/"):
        metadata_file = directory / _PROJECT_METADATA_FILE_NAME

        if metadata_file.exists():
            return metadata_file

        directory = directory.parent

    raise ProjectInspectionError(
        f"Could not find '{_PROJECT_METADATA_FILE_NAME}' file"
    )


def _load_project_metadata(file: Path) -> Mapping[str, Any]:
    with file.open("rb") as stream:
        return tomllib.load(stream)


def _get_project_version(metadata: Mapping[str, Any]) -> Version:
    return Version.parse(metadata["tool"]["poetry"]["version"])


def _list_migration_script_files(
    project_root_directory: Path
) -> list[tuple[Version, Path]]:
    migration_directory = (
        project_root_directory / _PROJECT_MIGRATION_DIRECTORY_NAME
    )

    candidate_files = migration_directory.glob("*.py")
    migration_files = map(_to_versioned, candidate_files)

    return list(migration_files)


def _calculate_migration_strategy(
    current_version: Version,
    target_version: Version,
    available_versions: Iterable[Version]
) -> _MigrationStrategy | None:    
    version_comparison = current_version.compare(target_version)

    if version_comparison == 0:
        return None
    elif version_comparison == -1:
        direction = _MigrationDirection.FORWARD
        selected_versions = filter(
            lambda x: current_version.compare(x) == version_comparison,
            available_versions
        )

        selected_versions = filter(
            lambda x: (
                target_version.compare(x) == -version_comparison or
                target_version.compare(x) == 0
            ),
            selected_versions
        )
    elif version_comparison == 1:
        direction = _MigrationDirection.BACKWARD
        selected_versions = filter(
            lambda x: current_version.compare(x) == version_comparison or
            current_version.compare(x) == 0,
            available_versions
        )

        selected_versions = filter(
            lambda x: target_version.compare(x) == -version_comparison,
            selected_versions
        )
    else:
        raise RuntimeError("Unexpected version comparison result")

    sorted_versions = sorted(
        selected_versions,
        key=cmp_to_key(lambda a, b: a.compare(b)), # type: ignore[attr-defined, misc]
        reverse=direction is _MigrationDirection.BACKWARD
    )

    return direction, sorted_versions


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

    return _Migration(**_fst(migrations))


async def _insert_migration(
    session: AsyncIOMotorClientSession,
    collection: AsyncIOMotorCollection,
    migration: _Migration
) -> None:
    await collection.insert_one(migration.dict(), session=session)


async def _insert_migration_of(
    session: AsyncIOMotorClientSession,
    collection: AsyncIOMotorCollection,
    version: Version
) -> None:
    await _insert_migration(session, collection, _Migration.of(version))


async def _run_migration_script(
    session: AsyncIOMotorClientSession,
    version: Version,
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
    migration_script_files = _list_migration_script_files(
        project_root_directory
    )

    async with await client.start_session() as session:
        async with session.start_transaction():
            database = _database(client)
            collection = _collection(database)

            last_migration = await _find_last_migration(session, collection)

            if last_migration is None:
                current_database_verstion = _INITIAL_DATABASE_VERSION
            else:
                current_database_verstion = last_migration.version

            strategy = _calculate_migration_strategy(
                current_database_verstion,
                current_project_version,
                map(_fst, migration_script_files) # type: ignore[arg-type]
            )

            if strategy is None:
                logger.info(
                    "No version drift between project (%s) and database (%s) "
                    "schema detected, everything is up to date",
                    current_project_version,
                    current_database_verstion
                )

                return

            direction, migration_versions = strategy

            logger.info(
                "Detected version drift between project (%s) and "
                "database schema (%s), applying (%s) migration",
                current_project_version,
                current_database_verstion,
                direction.value.lower()
            )

            for version in migration_versions:
                _, file = _find(
                    lambda x: _fst(x) == version,
                    migration_script_files
                )

                await _run_migration_script(
                    session,
                    version,
                    file,
                    direction
                )

            await _insert_migration_of(
                session,
                collection,
                current_project_version
            )

            logger.info(
                "Successfully applied %d migration scripts",
                len(migration_versions)
            )
