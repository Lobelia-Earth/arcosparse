# Contribution Guide

## Release Process

### 1. Releasing on PyPI

The package is distributed on PyPI and conda-forge.
We release from main always.

The release should be done this way (be sure to commit or stash your changes or they might be lost):

- `git checkout main && git fetch origin main && git reset --hard origin/main`
- `git checkout -b release-vx.y.z` (Use the version of the release or any branch name really)
- `make bump-version-patch` Bump version (change to minor or major if needed)
- Push and create a pull request
- Once the PR is merged go to actions and trigger the "Release" action

### 2. Release on conda-forge

After a few hours developers should receive a notification from conda-feedstock where they can deal with the PR and deploy on conda-forge.

Here is [the feedstock](https://github.com/conda-forge/arcosparse-feedstock) where the PR should be merged once the tests pass.

## Testing

The tests are run on GitHub actions and should be triggered on every PR and push to main.

### Authentication

Some tests require token to authenticate requests, in particular when it comes to ECMWF data.
To run those tests, you need to set up the `ARCOSPARSE_ECMWF_TOKEN` as environment variable or in a `.test.env` file in the `tests` folder.
See `tests/.test.env.template` for an example of the format of the file.

Tests concerned by authentication:

- `tests/test_get_entities.py`
