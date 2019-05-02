Spike 011: Initial Full Table to CDC Transition Strategy
========================================================

Should be informed by
[tap-postgres's](https://github.com/singer-io/tap-postgres) and
[tap-mysql's](https://github.com/singer-io/tap-mysql) strategy.

Need to be sure to send an
[initial](https://github.com/singer-io/tap-postgres/blob/390fc1148ff70dff40509992ac78c112363cf323/tap_postgres/sync_strategies/full_table.py#L95)
[activate version message](https://github.com/singer-io/tap-mysql/blob/5b466c2a4dc0d81a6cf66d1a0c740237cc6212b0/tap_mysql/sync_strategies/full_table.py#L205-L208)
on the initial full table so data trickles in.

Essentially, `tap-postgres` and `tap-mysql` save or elide some state to
indicate that they're doing their initial sync or their initial sync is
completed. I think it would be slightly more elegant in the Clojure world
to always 'do' the full table sync, but based on the current state emit an
empty sequence of records since we're actually done.

In the case of `tap-postgres`, the state in question is `xmin`. During the
full table sync it's
[written to at intervals until the sync finishes](https://github.com/singer-io/tap-postgres/blob/390fc1148ff70dff40509992ac78c112363cf323/tap_postgres/sync_strategies/full_table.py#L138-L148).
If the sync is interrupted it is
[used to indicate that the sync should resume](https://github.com/singer-io/tap-postgres/blob/390fc1148ff70dff40509992ac78c112363cf323/tap_postgres/sync_strategies/full_table.py#L118).
Once the sync has completed
[it's cleared](https://github.com/singer-io/tap-postgres/blob/390fc1148ff70dff40509992ac78c112363cf323/tap_postgres/__init__.py#L570).

In the case of `tap-mysql`, the state in question is a conglomeration of
[`log_file`, `log_pos`, `max_pk_values`, and `last_pk_fetched`](https://github.com/singer-io/tap-mysql/blob/5b466c2a4dc0d81a6cf66d1a0c740237cc6212b0/tap_mysql/__init__.py#L347-L367).
If `log_file` and `log_pos` are present and `max_pk_values` and
`last_pk_fetched` are not then the initial sync is done. This is managed
by
[`do_sync_historical_binlog`](https://github.com/singer-io/tap-mysql/blob/5b466c2a4dc0d81a6cf66d1a0c740237cc6212b0/tap_mysql/__init__.py#L528-L599).

The goal of the code here is to have a unit test essentiall that shows
state mutating appropriately. No matter how we solve this problem it will
have something to do with state as that's all we get passed in between
runs.
