import migraine
import pytest

from semver import Version


@pytest.mark.parametrize(
    "current_version, target_version, available_versions, expected_result",
    [
        (
            "0.0.0",
            "0.0.1",
            [],
            (
                migraine._MigrationDirection.FORWARD,
                []
            )
        ),
        (
            "0.0.0",
            "0.0.1",
            ["0.0.1"],
            (
                migraine._MigrationDirection.FORWARD,
                ["0.0.1"]
            )
        ),
        (
            "0.0.0",
            "0.0.2",
            ["0.0.1", "0.0.2"],
            (
                migraine._MigrationDirection.FORWARD,
                ["0.0.1", "0.0.2"]
            )
        ),
        (
            "0.0.1",
            "0.0.1",
            [],
            None
        ),
        (
            "0.0.1",
            "0.0.1",
            ["0.0.1"],
            None
        )
    ]
)
def test_calculate_migration_strategy(
    current_version: str,
    target_version: str,
    available_versions: list[str],
    expected_result: tuple[migraine._MigrationDirection, list[str]] | None
) -> None:
    if expected_result is not None:
        expected_result = (
            expected_result[0],
            list(map(Version.parse, expected_result[1]))
        )

    actual_result = migraine._calculate_migration_strategy(
        Version.parse(current_version),
        Version.parse(target_version),
        list(map(Version.parse, available_versions))
    )

    assert actual_result == expected_result
