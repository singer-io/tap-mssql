# MS-SQL Tap Config Settings

_This page documents the various settings supported by `tap-mssql`._

## Available Settings

| setting  | description                               |
| -------- | ----------------------------------------- |
| host     | The SQL Server IP address or server name. |
| port     | The port to use when connecting.          |
| database | The database name.                        |
| user     | The user name for connection.             |
| password | The user name for connection.             |
| ssl      | 'True' to use SSL, otherwise 'False.      |

* ***[TK - TODO: How is log-based replication configured?]***
* ***[TK - TODO: Any other configurable settings?]***

## Sample `settings.json` file

```json
{
  "host": "mytestsqlserver.cqaqbfvfo67k.us-east-1.rds.amazonaws.com",
  "port": "1433",
  "database": "sales_db",
  "user": "automation_user",
  "password": "t0p-sEcr3t!",
  "ssl": true
}
```
