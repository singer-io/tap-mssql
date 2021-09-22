# Installation Guide

## Minimal Install (Run Only)

At minimum, the tap needs the following components working in order to run locally:

1. Java 8 ([JRE or JDK](https://stackoverflow.com/a/1906455/4298208) both okay [TK - TODO: Confirm this works with JRE])
2. [Leiningen](https://leiningen.org/) ([Clojure](https://clojure.org/) execution framework and installer)

### Windows

_These instructions require the [Chocolatey](chocolatey.org) package manager to automate the install process._

1. Install Java:

    ```cmd
    # Install the runtime only:
    choco install javaruntime

    # OR install the full JDK:
    choco install jdk8
    ```

2. Install Leiningen

    ```cmd
    choco install lein
    ```

3. Install git (if not already installed)

    ```cmd
    choco install -y git.install --params "/GitOnlyOnPath /SChannel /NoAutoCrlf /WindowsTerminal"
    ```

4. Clone this repo

    ```cmd
    # Optionally, make a new directory:
    mkdir c:\Files\Source
    cd c:\Files\Source

    # Download the tap:
    git clone https://github.com/singer-io/tap-mssql.git
    ```

5. Test `tap-mssql` using `lein`:

    ```cmd
    # Change to the tap-mssql root directory:
    cd tap-mssql

    # Test using lein:
    lein run -m tap-mssql.core --config config.json --discover

    # NOTE: If working properly, at this point you should receive an error that config.json is not found.
    ```

* If you've gotten this far, you have successfully installed tap-mssql. You are ready to start [running the tap](../README.md#Running_the_tap).

### Mac

_These instructions require the [homebrew](brew.sh) package manager to automate the install process._

1. Install Java:

    ```cmd
    # Install the runtime only:
    brew install javaruntime

    # OR install the full JDK:
    brew install jdk8
    ```

2. Install Leiningen

    ```cmd
    brew install leiningen
    ```

3. Install git (if not already installed)

    ```cmd
    brew install git
    ```

4. Clone this repo

    ```cmd
    # Optionally, make a new directory:
    mkdir -p ~/Source
    cd ~/Source

    # Download the tap:
    git clone https://github.com/singer-io/tap-mssql.git
    ```

5. Test `tap-mssql` using `lein`:

    ```cmd
    # Change to the tap-mssql root directory:
    cd tap-mssql

    # Test using lein:
    lein run -m tap-mssql.core --config config.json --discover

    # NOTE: If working properly, at this point you should receive an error that config.json is not found.
    ```

* If you've gotten this far, you have successfully installed tap-mssql. You are ready to start [running the tap](../README.md#Running_the_tap).

### Linux (Ubuntu)

This tap has been consistently tested to run using `OpenJDK 8`, which can be installed on Ubuntu using these commands.

```bash
apt-get update && apt-get install -y openjdk-8-jdk
```

## Dev Environment Setup

- TK - TODO: What additional installation or config needed for dev users?
