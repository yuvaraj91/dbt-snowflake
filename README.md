# dbt-snowflake
Testing dbt with the Snowflake 30-day free trial


## Local setup

To interact with Snowflake from the local machine, first run the following to install the dbt adapter

* Run `make init-venv` to install the dependencies.
* Activate the virtualenv by running `source ./venv/bin/activate`.

Setup your Snowflake credentials in `~/.dbt/profiles.yml`. The sample structure looks like this:

```
snowflake:
  outputs:
    dev:
      account: 
      database: 
      password: 
      role: 
      schema: 
      threads: 1
      type: snowflake
      user:
      warehouse: 
  target: dev
```

## Running dbt locally

First change the working directory with `cd snowflake` before running and dbt cli commands.

For example, you can create a table public.places by running

```
dbt seed --select places
```

## Install Metricflow

Install metricflow, et al within a virtual environment:
```shell
python -m venv .venv
source .venv/bin/activate
pip install "dbt-metricflow[snowflake]"
dbt --version
mf --version
```

## Test the connection
1. Update the `profile` within `dbt_project.yml` to refer to one of your pre-existing profile

```shell
dbt debug
```

## Load data

```shell
dbt deps
```

## Run your dbt project, and query metrics

```shell
dbt build --exclude path:jaffle-data
mf validate-configs
mf query --metrics aov_eur
mf query --metrics aov_eur --explain
```

# Query the Semantic Layer from Python

- Build the project requirements by running `python -m pip install path/to/project`
- Add the environment variable to your path (bash / .zshrc) file
export DBT_JDBC_URL="<token here>"

Sample run command:

```
python src/adbc_example.py 'select * from {{ semantic_layer.query(metrics=["downsell_2023"]) }}'
```
