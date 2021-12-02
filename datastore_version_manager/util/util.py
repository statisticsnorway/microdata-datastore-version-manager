def bump_draft_version(semver: str) -> str:
    [major, minor, patch, draft_version] = semver.split('.')
    bumped_draft_version = int(draft_version) + 1
    return f'{major}.{minor}.{patch}.{bumped_draft_version}'
