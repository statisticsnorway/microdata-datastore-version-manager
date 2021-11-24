def bump_draftpatch_version(semver: str) -> str:
    [major, minor, patch, draftpatch] = semver.split('.')
    bumped_draftpatch = int(draftpatch) + 1
    return f'{major}.{minor}.{patch}.{draftpatch}'