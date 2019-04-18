Dev/Circle/tap-tester mssql instance options
============================================

Investigate options for getting test instances for mssql.

Primary options (in order of preference) are:

1. Cloud Hosted
1. Docker
1. Linux Installation

Requirements
------------

- Allow running all supported versions
- Allow turning CDC on/off

AWS RDS
-------

From
[the docs](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.SQLServer.CommonDBATasks.CDC.html)

> Using Change Data Capture
>
> Amazon RDS supports change data capture (CDC) for your DB instances
> running Microsoft SQL Server. CDC captures changes that are made to the
> data in your tables. It stores metadata about each change, which you can
> access later. For more information about how CDC works, see Change Data
> Capture in the Microsoft documentation.
>
> Before you use CDC with your Amazon RDS DB instances, enable it in the
> database by running `msdb.dbo.rds_cdc_enable_db`. After CDC is enabled,
> any user who is `db_owner` of that database can enable or disable CDC on
> tables in that database.
>
> **Important**
>
> During restores, CDC will be disabled. All of the related metadata is
> automatically removed from the database. This applies to snapshot
> restores, point-in-time restores, and SQL Server Native restores from S3.
> After performing one of these types of restores, you can re-enable CDC and
> re-specify tables to track.
>
> ```
> --Enable CDC for RDS DB Instance
> exec msdb.dbo.rds_cdc_enable_db '<database name>'
> ```
>
> To disable CDC, `msdb.dbo.rds_cdc_disable_db` run .
>
> ```
> --Disable CDC for RDS DB Instance
> exec msdb.dbo.rds_cdc_disable_db '<database name>'
> ```

It looks to be impossible to set up CDC on an express edition DB.

```
vagrant@taps-tvisher1:~$ mssql-cli -U spike_tap_mssql -P spike_tap_mssql -S spike-tap-mssql.cqaqbfvfo67k.us-east-1.rds.amazonaws.com
Version: 0.15.0
Mail: sqlcli@microsoft.com
Home: http://github.com/dbcli/mssql-cli
master> \d
OBJECT is required. Usage '\d OBJECT'.

Time: 0.000s
master> \ld
+----------+
| name     |
|----------|
| master   |
| tempdb   |
| model    |
| msdb     |
| rdsadmin |
+----------+
(5 rows affected)
Time: 1.750s (a second)
master> create database "spike_tap_mssql"
Commands completed successfully.
Time: 2.157s (2 seconds)
master> exec msdb.dbo.rds_cdc_enable_db 'spike_tap_mssql'
Msg 50000, Level 16, State 1, Procedure msdb.dbo.rds_cdc_enable_db, Line 70
This instance of SQL Server is the Express Edition (64-bit). Change data capture is only available in the Enterprise, Developer, Enterprise Evaluation, and Standard editions.
Time: 1.131s (a second)
```

We can create the following server editions in RDS:

> SQL Server Express Edition
> Affordable database management system that supports database sizes up to 10 GiB.
>
> SQL Server Web Edition
> In accordance with Microsoft's licensing policies, it can only be used to support public and Internet-accessible webpages, websites, web applications, and web services.
>
> SQL Server Standard Edition
> Core data management and business intelligence capabilities for mission-critical applications and mixed workloads.
>
> SQL Server Enterprise Edition
> Comprehensive high-end capabilities for mission-critical applications with demanding database workloads and business intelligence requirements.

See [the
docs](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/SQLServer.Concepts.General.Licensing.html)
for further information.

Presumably this indicates successfully enabling CDC in RDS (against a
'Standard Edition' database).

```
vagrant@taps-tvisher1:~$ mssql-cli -U spike_tap_mssql -P spike_tap_mssql -S spike-tap-mssql-2.cqaqbfvfo67k.us-east-1.rds.amazonaws.com
Version: 0.15.0
Mail: sqlcli@microsoft.com
Home: http://github.com/dbcli/mssql-cli
master> create database "spike_tap_mssql"
Commands completed successfully.
Time: 0.359s
master> exec msdb.dbo.rds_cdc_enable_db 'spike_tap_mssql'
CDC enabled on database spike_tap_mssql
Time: 0.512s
master>
```

Based on testing RDS works so we'll go with that since it was our
preferred option.
