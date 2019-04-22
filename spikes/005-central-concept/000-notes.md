Establish Central Concept

For https://stitchdata.atlassian.net/browse/SRCE-861

> How do we want to structure the stream output?
>
> lazy sequences processed centrally?
>
> core.async?
>
> in-situ stateful (println based)
>
> Be mindful of:
>
> State messages that should only be emitted after records are sent
>
> Schema messages that need to be emitted before records
>
> Records being sent in order
>
> Transformation
>
> State updates (in memory)

The results of this spike should be considered
[the lazy-seq implementation](https://github.com/stitchdata/tap-mssql/blob/bd1a53447b53f14b211d04c1b5b35da149005063/spikes/005-central-concept/tap-const/src/tap_const/lazy_seq.clj#L1)
which satisfies all the requirements.

