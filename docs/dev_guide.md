# Developers Guide

Associated tooling required to use the scripts in this repository follow. (Running the latest versions)

- [**Leiningen**](https://leiningen.org/)
- [**Docker (for integration tests)**](https://www.docker.com/)
- [**MSSQL CLI (to connect to test database)**](https://docs.microsoft.com/en-us/sql/tools/mssql-cli?view=sql-server-2017)

## Quick Start

```bash
$ bin/tap-mssql --config config.json --discover > catalog.json
$ bin/tap-mssql --config config.json --catalog catalog.json --state state.json | target...
```

## Usage

In the `bin` folder, there are a few utility scripts to simplify interacting with this tap. Many of these scripts rely on some environment variables being set, see "Testing Infrastructure Design" for more information.

**bin/tap-mssql** - This script wraps the `lein` command to run the tap from source code. It is analogous to the command installed by setuptools in Python taps.

As this is a Clojure tap, it supports a non-standard mode of operation by passing the `--repl` flag. This will start an NREPL server and log the port that it is running on to connect from an IDE for REPL driven development. It is compatible with all other command-line arguments, or can be used on its own. If the tap is invoked in discovery or sync mode along with `--repl`, the process will be kept alive after the usual Singer process is completed.

```bash
Example:
# Discovery
$ bin/tap-mssql --config config.json --discover > catalog.json

# Sync
$ bin/tap-mssql --config config.json --catalog catalog.json --state state.json

# REPL Mode
$ bin/tap-mssql --config config.json --repl
```

**bin/test** - This script wraps `lein test` in order to run the Clojure unit and integration tests against a database running locally.

```bash
Example:
$ bin/test
```

**bin/test-db** - This script uses docker to run a SQL Server container locally that can be used to run the unit tests against. See the usage text for more information.

Note: It also depends on the `mssql-cli` tool being installed in order to use the `connect` option.

```bash
# Example:
$ bin/test-db start
$ bin/test-db connect
$ bin/test-db stop
```

**bin/circleci-local** - This script wraps the [`circleci` CLI tool](https://circleci.com/docs/2.0/local-cli/) to run the Clojure unit and integration tests in the way CircleCI does, on localhost.

```bash
Example:
$ bin/circleci-local
```

## Testing Infrastructure Design

Each actor (developer, CI, etc.) needs their own testing infrastructure so
that development can proceed and be verified independently of each other.
In order to provide this isolation, we've migrated towards a Docker-based
solution.

A script, `bin/test-db` has been provided that will honor several
environment variables and manage the container required by the development
and testing.

The environment variables are:

| name | description |
| --- | --- |
| `STITCH_TAP_MSSQL_TEST_DATABASE_USER` | The admin user that should be used to connect to the test database (for docker, this is SA) |
| `STITCH_TAP_MSSQL_TEST_DATABASE_PASSWORD` | The password for the user (if docker, the SA user will be configured with this password) |
| `STITCH_TAP_MSSQL_TEST_DATABASE_PORT` | The port for hosting the server. (Default 1433)|

To interact with the container, these commands are available:

`bin/test-db start` - Starts the container under the name `sql1`

`bin/test-db connect` - Uses `mssql-cli` to open a shell to the local MSSQL instance

`bin/test-db stop` - Tears down and removes the container

**Note:** There is no volume binding, so all of the data and state in the
  running container is entirely ephemeral

## Troubleshooting

For help common errors, please see the [troubleshooting guide](docs/troubleshooting.md).
