# tap-mssql

`tap-mssql` is a Singer tap for MSSQL.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`
* `batch`

## Supported Python Versions

* 3.8
* 3.9
* 3.10
* 3.11
* 3.12

A full list of supported settings and capabilities for this
tap is available by running:

## Settings

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| host | False    | None    | Host for SQL Server Instance. |
| database | False    | None    | Database to connect to. |
| port | False    |    1433 | The port of the SQL Server Instance. |
| username | False    | None    | Username used to authenticate. |
| password | False    | None    | Password used to authenticate. |
| sqlalchemy_url_query_options | False    | None    | List of SQLAlchemy URL Query options to provide. Example: driver, TrustServerCertificate, etc. |
| sqlalchemy_url_query | False    | None    | SQLAlchemy URL. Setting this will take precedence over other connection settings. |
| default_replication_method | False    | FULL_TABLE | Replication method to use if there is not a catalog entry to override this choice. One of `FULL_TABLE`, `INCREMENTAL`, or `LOG_BASED`. |
| stream_maps | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config | False    | None    | User-defined config values to be used within map expressions. |
| faker_config | False    | None    | Config for the [`Faker`](https://faker.readthedocs.io/en/master/) instance variable `fake` used within map expressions. Only applicable if the plugin specifies `faker` as an addtional dependency (through the `singer-sdk` `faker` extra or directly). |
| faker_config.seed | False    | None    | Value to seed the Faker generator for deterministic output: https://faker.readthedocs.io/en/master/#seeding-the-generator |
| faker_config.locale | False    | None    | One or more LCID locale strings to produce localized output for: https://faker.readthedocs.io/en/master/#localization |
| flattening_enabled | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False    | None    | The max depth to flatten schemas. |
| batch_config | False    | None    |             |
| batch_config.encoding | False    | None    | Specifies the format and compression of the batch files. |
| batch_config.encoding.format | False    | None    | Format to use for batch files. |
| batch_config.encoding.compression | False    | None    | Compression format to use for batch files. |
| batch_config.storage | False    | None    | Defines the storage layer to use when writing batch files |
| batch_config.storage.root | False    | None    | Root path to use when writing batch files. |
| batch_config.storage.prefix | False    | None    | Prefix to use when writing batch files. |

A full list of supported settings and capabilities is available by running: `tap-mssql --about`

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

This tap utilizes [pyodbc](https://pypi.org/project/pyodbc/) as the SQL Server driver. You must have a compatible
version of the Microsoft ODBC Driver installed on the host machine. Installation instructions are available
on [Microsoft](https://learn.microsoft.com/en-us/sql/connect/odbc/microsoft-odbc-driver-for-sql-server?view=sql-server-ver16).

For SQL Alchemy to connect to the database, the driver has to be specified in the connection string.

There are two ways to accomplish this.

First, by providing it in the `sqlalchemy_url_query_options`:

```yaml
sqlalchemy_url_query_options:
  - key: driver
    value: ODBC Driver 18 for SQL Server
```

Or by providing it as part of the `sqlalchemy_url_query` configuration property.
 
```text
mssql+pyodbc://sa:***@localhost:54198/databasename?driver=ODBC+Driver+18+for+SQL+Server
```


## Usage

You can easily run `tap-mssql` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-mssql --version
tap-mssql --help
tap-mssql --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-mssql` CLI interface directly using `poetry run`:

```bash
poetry run tap-mssql --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-mssql
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-mssql --version
# OR run a test `elt` pipeline:
meltano elt tap-mssql target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.

### How to Set Up Log-Based Replication (Change Tracking)

A great guide for setting up change tracking for a database is available on [Stitch](https://www.stitchdata.com/docs/integrations/databases/microsoft-sql-server/v1#extract-data)
