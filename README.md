# dbt-snowflake

Testing dbt with the Snowflake 30-day free trial

- Using the sample data provided in the TPCH_SF1 schema.
- Dbt Semantic layer via MetricFlow
- CI/CD pipeline in GitHub Actions


## Local setup

To interact with Snowflake from the local machine, first run the following to install the dbt adapter

- Run `make init-venv` to install the dependencies.
- Activate the virtualenv by running `source ./venv/bin/activate`.

Setup your Snowflake credentials in `~/.dbt/profiles.yml`. The sample structure looks like this:

```
snowflake:
  outputs:
    dev:
      account: 
      database: 
      password: 
      role: 
      schema: public
      threads: 1
      type: snowflake
      user:
      warehouse: 
  target: dev
```

## Test the connection

Update the `profile` within `dbt_project.yml` to refer to one of your pre-existing profile

```shell
dbt debug
```

## Running dbt locally

For example, you can create a table public.places by running

```
dbt seed --select places
```

## Load data

Based on the Snowflake sample data in the TPCH_SF1 schema.

```shell
dbt deps
dbt build
```

## Documentation and lineage

Spins up a local container to serve the dbt docs in a web-browser - `localhost:8081`.
Under the hood executes `dbt docs generate` and `dbt docs serve`.

```shell
make dbt-docs
```

# Semantic Layer and MetricFlow

- Run your dbt project, and query metrics*

```shell
dbt build --exclude path:jaffle-data
mf validate-configs
mf query --metrics revenue
mf query --metrics revenue --explain
```

- Query the Semantic Layer from Python

Build the project requirements by running `python -m pip install path/to/project`
Add the environment variable to your path (bash / .zshrc) file

`export DBT_JDBC_URL="<token here>"`

Sample run command:

```
python src/adbc_example.py 'select * from {{ semantic_layer.query(metrics=["downsell_2023"]) }}'
```
