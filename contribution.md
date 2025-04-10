# Release Process

## 1. Releasing on PyPI

The package is distributed on PyPI and conda-forge.
We release from main always.

The release should be done this way (be sure to commit or stash your changes or they might be lost):

- `git checkout main && git fetch origin main && git reset --hard origin/main`
- `git checkout -b release-vx.y.z` (Use the version of the release or any branch name really)
- `make bump-version-patch` Bump version (change to minor or major if needed)
- Push and create a pull request
- Once the PR is merged go to actions and trigger the "Release" action

## 2. Release on conda-forge

After a few hours developers should receive a notification from conda-feedstock where they can deal with the PR and deploy on conda-forge.

Here is [the feedstock](https://github.com/conda-forge/arcosparse-feedstock) where the PR should be merged once the tests pass.
