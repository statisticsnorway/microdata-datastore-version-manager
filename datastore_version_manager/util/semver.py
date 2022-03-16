def bump_major_version(semver: str) -> str:
    return bump_version(semver, 0)


def bump_minor_version(semver: str) -> str:
    return bump_version(semver, 1)


def bump_patch_version(semver: str) -> str:
    return bump_version(semver, 2)


def bump_draft_version(semver: str) -> str:
    return bump_version(semver, 3)


def bump_version(semver: str, index: int) -> str:
    semver_list = semver.split('.')
    semver_list[index] = str(int(semver_list[index]) + 1)
    return '.'.join(semver_list)


def dotted_to_underscored(semver: str) -> str:
    return semver.replace('.', '_')


def calculate_new_version(data_structure_updates: list,
                          previous_version: str = None) -> tuple[str, str]:
    operations = [
        data_structure["operation"]
        for data_structure in data_structure_updates
        if data_structure["releaseStatus"] in [
            "PENDING_RELEASE", "PENDING_DELETE"
        ]
    ]

    if not operations:
        return "", ""

    if "CHANGE_DATA" in operations or "REMOVE" in operations:
        return (
            "MAJOR", bump_major_version(previous_version)
            if previous_version is not None else ""
        )
    elif "ADD" in operations:
        return (
            "MINOR", bump_minor_version(previous_version)
            if previous_version is not None else ""
        )
    elif "PATCH_METADATA" in operations:
        return (
            "PATCH", bump_patch_version(previous_version)
            if previous_version is not None else ""
        )
    else:
        raise InvalidOperation(
            f"Invalid operation in {operations}"
        )


class InvalidOperation(Exception):
    pass
