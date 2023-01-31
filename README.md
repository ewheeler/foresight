
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

On an M1 mac, installing the `darts` package via conda and poetry failed,
so it is included in `pyproject.toml` with an environment marker for `x86_64`
systems. M1 mac users will need to `pip install darts` :(


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

To persist dagit state after the process exits, set a `DAGSTER_HOME` env var:
```
$ export DAGSTER_HOME='/path/to/src/foresight'
```

By default, pipelines will write datasets to the local filesystem.
To write datasets to a GCP bucket instead, change the 'mode' with an env var:
```
$ export DAGSTER_DEPLOYMENT='production'
```

To enable backfill and scheduling functionality, set `DAGSTER_HOME`
in another shell and run:
```
$ dagster-daemon run
```

Dagster includes support for using `.env` files
https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets#declaring-environment-variables
So an `example.env` is included in the repo.
Make a copy and then customize:
```
$ cd src/foresight
$ cp example.env .env
```


### Notebooks

Notebooks are stored as Jupytext Paired notebooks ('light' .py files):
https://jupytext.readthedocs.io/en/latest/paired-notebooks.html

The jupyter extension should be installed by Poetry, but you may
need to enable it: https://jupytext.readthedocs.io/en/latest/install.html#jupytext-menu-in-jupyter-notebook

Also, the Jupytext menu described in the link above doesn't show for me,
but going to `View > Activate Command Palette` menu in Jupyterlab 
and selecting 'Pair Notebook with light Script' works for pairing a new '.ipynb'


###  Cloud

Cloud resources are provisioned and managed with Pulumi

First, install on your system.
On macos:
```
$ brew install pulumi
```

Configure gcp project and pulumi state storage location
```
$ pulumi config set gcp:project foresight-375620
$ pulumi config set gcp:region us-central1
$ pulumi config set gcp:zone us-central1-a
$ pulumi login gs://frsght-pulumi-state
```

Bring up stack (VM, firewall, storage)
```
$ cd cloud
$ pulumi up
```

SSH into VM
```
gcloud compute ssh $(pulumi stack output instanceName)
```

Put GCP service account token onto VM
```
gcloud compute scp ../src/foresight/foresight-375620-01e36cd13a77.json $(pulumi stack output instanceName):/home/frsght/foresight/src/foresight
```

SSH with port forwarding of dagit ui port
```
gcloud compute ssh $(pulumi stack output instanceName) --ssh-flag="-N" --ssh-flag="-L 3000:localhost:3000"
```

Destroy stack
```
$ pulumi destroy
```
