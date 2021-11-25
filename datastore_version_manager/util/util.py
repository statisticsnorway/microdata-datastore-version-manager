def bump_draftpatch(semver: str) -> str:
    [major, minor, patch, draftpatch] = semver.split('.')
    bumped_draftpatch = int(draftpatch) + 1
    return f'{major}.{minor}.{patch}.{bumped_draftpatch}'
