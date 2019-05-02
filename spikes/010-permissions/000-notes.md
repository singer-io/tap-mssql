This spike is for establishing a decent minimal set of permissions for how
to do discovery and syncing for mssql.

From the stitch docs:

> To set up MSSQL in Stitch, you need:
>
> **Permissions in MSSQL that allow you to create/manage users.** This is
> required to create the Stitch database user.
>
> A server that:
>
> - Uses case-insensitive collation. More info about collation can be
>   found here in Microsoft’s documentation.
> - Allows connections over TCP/IP
> - Allows mixed mode authentication
>
> **Make sure your server is set up properly before continuing.** If you
> need some help figuring out your hosting details, we recommend looping
> in a member of your engineering team.
