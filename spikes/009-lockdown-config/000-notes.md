The config values should probably be basically a union between tap-mysql/tap-postgresql
and db reps mssql.

```
+------------------------------+----------------------+-------------+---------------+-----------------+-------------+---------------------------------------------------------------------+---------------+---------+
| name                         | environment_variable | is_required | setup_step_id | system_provided | tap_mutable | json_schema                                                         | property_type | ordinal |
+------------------------------+----------------------+-------------+---------------+-----------------+-------------+---------------------------------------------------------------------+---------------+---------+
| host                         | NULL                 |           1 |            47 |               0 |           0 | {"type":"string","anyOf":[{"format":"hostname"},{"format":"ipv4"}]} | user_provided |    NULL |
| port                         | NULL                 |           1 |            47 |               0 |           0 | {"type":"string","pattern":"^\\d+"}                                 | user_provided |    NULL |
| user                         | NULL                 |           1 |            47 |               0 |           0 | {"type":"string"}                                                   | user_provided |    NULL |
| password                     | NULL                 |           1 |            47 |               0 |           0 | {"type":"string"}                                                   | user_provided |    NULL |
| database                     | NULL                 |           0 |            47 |               0 |           0 | {"type":"string"}                                                   | user_provided |    NULL |
| image_version                | NULL                 |           1 |            47 |               1 |           0 | NULL                                                                | read_only     |    NULL |
| frequency_in_minutes         | NULL                 |           0 |            47 |               0 |           0 | {"type": "string", "pattern": "^1$|^30$|^60$|^360$|^720$|^1440$"}   | user_provided |    NULL |
| anchor_time                  | NULL                 |           0 |            47 |               0 |           0 | {"type": "string", "format": "date-time"}                           | user_provided |    NULL |
| cron_expression              | NULL                 |           0 |            47 |               0 |           0 | NULL                                                                | user_provided |    NULL |

# Introduce ssh stuff as late as possible
# | ssh                          | NULL                 |           0 |            47 |               0 |           0 | {"type":"string","pattern":"^(true|false)"}                         | user_provided |    NULL |
# | ssh_host                     | NULL                 |           0 |            47 |               0 |           0 | {"type":"string","anyOf":[{"format":"hostname"},{"format":"ipv4"}]} | user_provided |    NULL |
# | ssh_port                     | NULL                 |           0 |            47 |               0 |           0 | {"type":"string","pattern":"^\\d+"}                                 | user_provided |    NULL |
# | ssh_user                     | NULL                 |           0 |            47 |               0 |           0 | {"type":"string"}                                                   | user_provided |    NULL |
# | ssl                          | NULL                 |           0 |            47 |               0 |           0 | {"type":"string","pattern":"^(true|false)"}                         | user_provided |    NULL |

# Looks to be used between mysql and postgres so maybe useful here? Should introduce later though.
# | filter_dbs                   | NULL                 |           0 |            47 |               0 |           0 | {"type":"string"}                                                   | user_provided |    NULL |
# Looks to be unused anywhere
# | use_log_based_replication    | NULL                 |           0 |            47 |               0 |           0 | {"type":"string","pattern":"^(true|false)$"}                        | user_provided |    NULL |
# Looks to be specific to mysql
# | server_id                    | NULL                 |           0 |            47 |               0 |           0 | {"type":"string","pattern":"^\\d+$"}                                | user_provided |    NULL |
# Introduce the following three as late as possible
# | ssl_cert                     | NULL                 |           0 |            47 |               0 |           0 | {"type":"string"}                                                   | user_provided |    NULL |
# | ssl_key                      | NULL                 |           0 |            47 |               0 |           0 | {"type":"string"}                                                   | user_provided |    NULL |
# | ssl_ca                       | NULL                 |           0 |            47 |               0 |           0 | {"type":"string"}                                                   | user_provided |    NULL |

# Looks to be specific to mysql
# | check_hostname               | NULL                 |           0 |            47 |               0 |           0 | {"type":"string","pattern":"^(true|false)"}                         | user_provided |    NULL |
# Looks to be specific to mysql
# | verify_mode                  | NULL                 |           0 |            47 |               0 |           0 | {"type":"string","pattern":"^(true|false)"}                         | user_provided |    NULL |
# Doesn't appear to be used in the tap but it's in the docs?
# | ssl_client_auth_enabled      | NULL                 |           0 |            47 |               0 |           0 | {"type":"string","pattern":"^(true|false)"}                         | user_provided |    NULL |
# Unused
# | allow_non_auto_increment_pks | NULL                 |           0 |            47 |               0 |           0 | {"type":"string","pattern":"^(true|false)$"}                        | user_provided |    NULL |
+------------------------------+----------------------+-------------+---------------+-----------------+-------------+---------------------------------------------------------------------+---------------+---------+
```
