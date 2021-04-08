# tap-mssql

[![CircleCI](https://circleci.com/gh/singer-io/tap-mssql.svg?style=svg)](https://circleci.com/gh/singer-io/tap-mssql)

[Singer](https://www.singer.io/) tap that extracts data from a [Microsoft SQL Server (MSSQL)](https://www.microsoft.com/en-us/sql-server/default.aspx) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

## Requirements

This tap is written in [Clojure](https://clojure.org/), and as such, requires Java and [Leiningen](https://leiningen.org/).

* For full installation instructions, please see the [installation guide](docs/installation.md).

## Onboarding for Developers and Testers

To get started as a contributor and/or tester, see the [contribution guidelines](docs/CONTRIBUTING.md) and [developer guide](docs/dev_guide.md).

## Configuring `tap-mssql`

At minimum, the tap requires the following settings: `host`, `port`, `database`, `username`, and `password`.

* For detailed configuration instructions, including a list of supported settings, please check the [configuration guide](docs/config.md).

## Running the tap

The tap supports several different wrappers and execution patterns for different types of environments.

### Running in production

When executing in production, the following patterns are generally recommended:

1. Executing using `lein` directly (platform agnostic):

    ```bash
    # Discover metadata catalog:
    lein run -m tap-mssql.core --config config.json --discover > catalog.json

    # Execute sync to target-csv (for example):
    lein run -m tap-mssql.core --config config.json --sync | target-csv > state.json
    ```

2. Executing using the `tap-mssql` shell script wrapper (Linux/Mac only):

    ```bash
    # Discover metadata catalog:
    bin/tap-mssql --config config.json --discover > catalog.json

    # Execute sync to target-csv (for example):
    bin/tap-mssql  --config config.json --sync | target-csv > state.json
    ```

3. Instructions to execute using docker:
    * ***TK - TODO: Which image name to use in place of `local/tap-mssql` below?***
    * Build the docker image:

        ```bash
        docker build -t local/tap-mssql .
        ```

    * Run using docker:

        ```bash
        # Discover metadata catalog:
        docker run --rm -it -v .:/home/tap-mssql local/tap-mssql --config config.json --discover > catalog.json

        # Execute sync to target-csv (for example):
        docker run --rm -it -v .:/home/tap-mssql local/tap-mssql --config config.json --sync | target-csv > state.json
        ```

### Other ways to run and test

For more ways to execute the tap, including dockerized methods, REPL methods, and various other testing configurations, see the the [Developers Guide](docs/dev_guide.md).

## Troubleshooting

For help common errors, please see the [troubleshooting guide](docs/troubleshooting.md).

---

Copyright &copy; 2019 Stitch
