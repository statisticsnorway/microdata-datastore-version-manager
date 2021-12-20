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
