Postgres has a concept of xmin associated with each row which makes every
table able to be synced incrementally. Does that concept exist in mssql?
Can we lean on it to do incremental full table or do we need to take the
mysql approach of requiring orderable primary keys.

-----------

It appears that SQL Server does not have an automatically available `xmin`
column so the strategy for doing interruptible full table syncing will
have to be the same as
[tap-mysql](https://github.com/singer-io/tap-mysql/blob/5b466c2a4dc0d81a6cf66d1a0c740237cc6212b0/tap_mysql/sync_strategies/full_table.py#L48-L82).

One tidbit from my research would be that if there’s a `rowversion` column
on a table we should probably use that to capture updates and inserts to a
full table sync. That kind of column doesn't exist in postgres.
