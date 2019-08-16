# tap-mssql

## Observed error messages:

```
# Bad Host Message

The TCP/IP connection to the host charnock.org, port 51552 has failed.
Error: "connect timed out. Verify the connection properties. Make sure
that an instance of SQL Server is running on the host and accepting
TCP/IP connections at the port. Make sure that TCP connections to the
port are not blocked by a firewall.".

# Unspecified azure server error message

Cannot open server "127.0.0.1" requested by the login. The login
failed. ClientConnectionId:33b6ae38-254a-483b-ba24-04d69828fe0c


# Bad dbname error message

Login failed for user 'foo'.
ClientConnectionId:4c47c255-a330-4bc9-94bd-039c592a8a31

# Database does not exist

Cannot open database "foo" requested by the login. The login
failed. ClientConnectionId:f6e2df79-1d72-4df3-8c38-2a9e7a349003
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
