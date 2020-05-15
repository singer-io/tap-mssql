# Changelog

## 1.6.2
  * Warn on permissions errors when discovering schemas if the user doesn't have access [#33](https://github.com/singer-io/tap-mssql/pull/33)

## 1.6.1
  * Fix bugs with Views being interrupted during a full table sync [#28](https://github.com/singer-io/tap-mssql/pull/28)

## 1.6.0
  * Configure ResultSet options for concurrency mode and cursor type [#25](https://github.com/singer-io/tap-mssql/pull/25)

## 1.5.2
  * Clarify error messages in edge case when change tracking is not available [#24](https://github.com/singer-io/tap-mssql/pull/24)
  * Fix edge case where two identically named tables in different schemas have different change tracking status [#24](https://github.com/singer-io/tap-mssql/pull/24)

## 1.5.1
  * Fix issue where some datetime types were having issues when approaching year 0. [#22](https://github.com/singer-io/tap-mssql/pull/22)

## 1.5.0
  * During query generation surround schema names with square brackets to allow reserved words in schemas. [#20](https://github.com/singer-io/tap-mssql/pull/20)

## 1.4.5
  * Added a fallback value for `_sdc_deleted_at` when running a log based sync.
  * The tap also logs when this happens.
  * [#18](https://github.com/singer-io/tap-mssql/pull/18)

## 1.4.4
  * Fixes a bug where during discovery, for columns of type `binary` the tap was writing the schema as a string, but not transforming the data to a string, instead emitting it as a byte array. [#16](https://github.com/singer-io/tap-mssql/pull/16)

## 1.4.3
  * Fix a bug where timestamp column bookmarks cause an exception when resuming full-table [#15](https://github.com/singer-io/tap-mssql/pull/15)

## 1.4.2
  * Extract database from config replacing it with an empty string if it is nil [#13](https://github.com/singer-io/tap-mssql/pull/13)

## 1.4.1
  * Removed max length from binary-type JSON schemas [#8](https://github.com/singer-io/tap-mssql/pull/8/)

## 1.4.0
  * Add support for connecting to Named Instances by omitting port and adding the instance name in the host field [#4](https://github.com/singer-io/tap-mssql/pull/4)

## 1.3.1
  * Fix discovery query to correctly type `sys.partitions.rows` as `bigint` [#5](https://github.com/singer-io/tap-mssql/pull/5)

## 1.3.0
  * Add support for money and smallmoney columns [#1](https://github.com/singer-io/tap-mssql/pull/1)

## 1.2.3
  * Fix the sql query generation for full table interruptible syncs with composite pks [#53](https://github.com/stitchdata/tap-mssql/pull/53)

## 1.2.2
  * Fix a bug with a View's key_properties being improperly set [#51](https://github.com/stitchdata/tap-mssql/pull/51)
  * Add an assertion to ensure log-based replication has the primary keys needed to replicate [#50](https://github.com/stitchdata/tap-mssql/pull/50)

## 1.2.1
  * Make _sdc_deleted_at nullable [commit](https://github.com/stitchdata/tap-mssql/commit/e95170bab642da301346cdf56485f8778d32ad2b)

## 1.2.0
  * Add support for datetime2, datetimeoffset, and smalldatetime [#49](https://github.com/stitchdata/tap-mssql/pull/49)

## 1.1.0
 * Add support for numeric and decimal identity columns [#48](https://github.com/stitchdata/tap-mssql/pull/48)

## 1.0.0
 * GA Release
