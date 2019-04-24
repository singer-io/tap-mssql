```
master> create database "spike_tap_mssql"
Commands completed successfully.
Time: 1.431s (a second)
master> exec msdb.dbo.rds_cdc_enable_db 'spike_tap_mssql'
CDC enabled on database spike_tap_mssql
Time: 0.558s
master> use "spike_tap_mssql";
Commands completed successfully.
Time: 0.257s
spike_tap_mssql> create table foo (id int primary key);
Commands completed successfully.
Time: 0.258s
spike_tap_mssql> insert into foo (id) values (1), (2), (3);
(3 rows affected)
Time: 0.257s
spike_tap_mssql> update foo set id = 4 where id = 2;
(1 row affected)
Time: 0.259s
spike_tap_mssql> select * from foo;
+------+
| id   |
|------|
| 1    |
| 3    |
| 4    |
+------+
(3 rows affected)
Time: 0.365s
spike_tap_mssql> exec sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foo', @role_name = null;
Job 'cdc.spike_tap_mssql_capture' started successfully.
Time: 7.435s (7 seconds)
```

Schemas are locked once the CDC table is created.
https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server?view=sql-server-2017
Could potentially use the DDL log to capture updates here.
https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-get-ddl-history-transact-sql?view=sql-server-2017

We may need to support netchanges.
https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/work-with-change-data-sql-server?view=sql-server-2017

> The function cdc.fn_cdc_get_net_changes_<capture_instance> is generated
> when the parameter @supports_net_changes is set to 1 when the source
> table is enabled.
>
> Note:
>
> This option is only supported if the source table has a defined primary
> key or if the parameter @index_name has been used to identify a unique
> index.
>
> The netchanges function returns one change per modified source table
> row. If more than one change is logged for the row during the specified
> interval, the column values will reflect the final contents of the row.
> To correctly identify the operation that is necessary to update the
> target environment, the TVF must consider both the initial operation on
> the row during the interval and the final operation on the row. When the
> row filter option 'all' is specified, the operations that are returned
> by a net changes query will either be insert, delete, or update (new
> values). This option always returns the update mask as null because
> there is a cost associated with computing an aggregate mask. If you
> require an aggregate mask that reflects all changes to a row, use the
> 'all with mask' option. If downstream processing does not require
> inserts and updates to be distinguished, use the 'all with merge'
> option. In this case, the operation value will only take on two values:
> 1 for delete and 5 for an operation that could be either an insert or an
> update. This option eliminates the additional processing needed to
> determine whether the derived operation should be an insert or an
> update, and can improve the performance of the query when this
> differentiation is not necessary.

```
spike_tap_mssql> select * from cdc.fn_cdc_get_all_changes_dbo_foo(sys.fn_cdc_get_min_lsn ( 'dbo_foo' ), sys.fn_cdc_get_max_lsn (), N'all');
(0 rows affected)
Time: 0.409s
spike_tap_mssql> insert into foo (id) values (7), (8), (9);
(3 rows affected)
Time: 0.262s
spike_tap_mssql> update foo set id = 2 where id = 4;
(1 row affected)
Time: 0.257s
spike_tap_mssql> insert into foo (id) values (4), (5), (6);
(3 rows affected)
Time: 0.260s
spike_tap_mssql> select * from cdc.fn_cdc_get_all_changes_dbo_foo(sys.fn_cdc_get_min_lsn ( 'dbo_foo' ), sys.fn_cdc_get_max_lsn (), N'all');
+------------------------+------------------------+----------------+------------------+------+
| __$start_lsn           | __$seqval              | __$operation   | __$update_mask   | id   |
|------------------------+------------------------+----------------+------------------+------|
| 0x00000028000008570005 | 0x00000028000008570002 | 2              | 0x01             | 7    |
| 0x00000028000008570005 | 0x00000028000008570003 | 2              | 0x01             | 8    |
| 0x00000028000008570005 | 0x00000028000008570004 | 2              | 0x01             | 9    |
| 0x00000028000008670006 | 0x00000028000008670002 | 1              | 0x01             | 4    |
| 0x00000028000008670006 | 0x00000028000008670002 | 2              | 0x01             | 2    |
+------------------------+------------------------+----------------+------------------+------+
(5 rows affected)
Time: 0.366s
spike_tap_mssql> select * from cdc.fn_cdc_get_all_changes_dbo_foo(sys.fn_cdc_get_min_lsn ( 'dbo_foo' ), sys.fn_cdc_get_max_lsn (), N'all');
+------------------------+------------------------+----------------+------------------+------+
| __$start_lsn           | __$seqval              | __$operation   | __$update_mask   | id   |
|------------------------+------------------------+----------------+------------------+------|
| 0x00000028000008570005 | 0x00000028000008570002 | 2              | 0x01             | 7    |
| 0x00000028000008570005 | 0x00000028000008570003 | 2              | 0x01             | 8    |
| 0x00000028000008570005 | 0x00000028000008570004 | 2              | 0x01             | 9    |
| 0x00000028000008670006 | 0x00000028000008670002 | 1              | 0x01             | 4    |
| 0x00000028000008670006 | 0x00000028000008670002 | 2              | 0x01             | 2    |
| 0x000000280000086F0005 | 0x000000280000086F0002 | 2              | 0x01             | 4    |
| 0x000000280000086F0005 | 0x000000280000086F0003 | 2              | 0x01             | 5    |
| 0x000000280000086F0005 | 0x000000280000086F0004 | 2              | 0x01             | 6    |
+------------------------+------------------------+----------------+------------------+------+
(8 rows affected)
Time: 0.368s
spike_tap_mssql> select * from cdc.fn_cdc_get_all_changes_dbo_foo(sys.fn_cdc_get_min_lsn ( 'dbo_foo' ), sys.fn_cdc_get_max_lsn (), N'all');
+------------------------+------------------------+----------------+------------------+------+
| __$start_lsn           | __$seqval              | __$operation   | __$update_mask   | id   |
|------------------------+------------------------+----------------+------------------+------|
| 0x00000028000008570005 | 0x00000028000008570002 | 2              | 0x01             | 7    |
| 0x00000028000008570005 | 0x00000028000008570003 | 2              | 0x01             | 8    |
| 0x00000028000008570005 | 0x00000028000008570004 | 2              | 0x01             | 9    |
| 0x00000028000008670006 | 0x00000028000008670002 | 1              | 0x01             | 4    |
| 0x00000028000008670006 | 0x00000028000008670002 | 2              | 0x01             | 2    |
| 0x000000280000086F0005 | 0x000000280000086F0002 | 2              | 0x01             | 4    |
| 0x000000280000086F0005 | 0x000000280000086F0003 | 2              | 0x01             | 5    |
| 0x000000280000086F0005 | 0x000000280000086F0004 | 2              | 0x01             | 6    |
+------------------------+------------------------+----------------+------------------+------+
(8 rows affected)
Time: 0.367s
spike_tap_mssql>
```
