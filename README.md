
## Installation

### Python environment

This package uses conda and poetry together.
(Thanks to https://stackoverflow.com/a/71110028)

Create conda environment with anything specific for the platform/architecture
by specifying the appropriate `conda*.lock` file


```
conda create --name my_project_env --file conda_platform_locks/conda-linux-64.lock
conda activate my_project_env
poetry install
```

Activating environment (poetry detects conda activation so don't need to
do that explicitly):

```
conda activate my_project_env
```

Updating environment (after running `poetry add new_dependency`):
```
# Re-generate Conda lock file(s) based on environment.yml
conda-lock -k explicit --conda mamba
# Update Conda packages based on re-generated lock file
mamba update --file conda_platform_locks/conda-linux-64.lock
# Update Poetry packages and re-generate poetry.lock
poetry update
```


### Documents

Install Quarto: https://quarto.org/docs/get-started/

To render docs to pdf, run:
```
quarto render
```
Look for a `.pdf` file in a new `_book` directory


### Pipelines

Navigate to foresight python package and launch Dagster's `dagit` UI
```
$ cd src/foresight
$ dagit
```
