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


def get_update_type(data_structure_updates: list) -> str:
    operations = [
        data_structure["operation"]
        for data_structure in data_structure_updates
    ]
    if "CHANGE_DATA" in operations or "REMOVE" in operations:
        return "MAJOR"
    elif "ADD" in operations:
        return "MINOR"
    elif "PATCH_METADATA":
        return "PATCH"
    else:
        raise RuntimeError(
            f"Invalid operation in {operations}"
        )