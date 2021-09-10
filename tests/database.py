import os
import socket
from pprint import pprint
from random import randint, sample

import pyodbc

USERNAME = os.getenv("STITCH_TAP_MSSQL_TEST_DATABASE_USER")
PASSWORD = os.getenv("STITCH_TAP_MSSQL_TEST_DATABASE_PASSWORD")
HOST = "localhost"
DATABASE_DEPS = ['msodbcsql17', 'unixodbc-dev']

LOWER_ALPHAS, UPPER_ALPHAS, DIGITS, OTHERS = set(), set(), set(), set()
for letter in range(97, 123):
    LOWER_ALPHAS.add(chr(letter))

for letter in range(65, 91):
    UPPER_ALPHAS.add(chr(letter))

for digit in range(48, 58):
    DIGITS.add(chr(digit))

for invalid in set().union(range(32, 48), range(58, 65), range(91, 97), range(123, 127)):
    OTHERS.add(chr(invalid))

ALPHA_NUMERIC = LOWER_ALPHAS.union(UPPER_ALPHAS).union(DIGITS)


def mssql_cursor_context_manager(*args):
    """Decorator to switch into the iFrame before the method and switch back out after"""

    server = "{},1433".format(HOST)
    database = "master"
    connection_string = (
        "DRIVER={{ODBC Driver 17 for SQL Server}}"
        ";SERVER={};DATABASE={};UID={};PWD={}".format(
            server, database, USERNAME, PASSWORD))

    print(connection_string.replace(PASSWORD, "[REDACTED]"))
    try:
        connection = pyodbc.connect(connection_string, autocommit=True)
    except pyodbc.Error as err:
        conection_error_code = '01000'
        conection_error_msg = 'ODBC Driver 17 for SQL Server'
        if err.args[0] == conection_error_code and conection_error_msg in err.args[1]:
            raise RuntimeError(f"Ensure you have the following dependencies installed! {DATABASE_DEPS}") from err

    # https://github.com/mkleehammer/pyodbc/wiki/Unicode#configuring-specific-databases
    # connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
    # connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    # connection.setencoding(encoding='utf-8')
    # connection.add_output_converter(-155, handle_datetimeoffset)

    with connection.cursor() as cursor:
        for q in args:
            print(q)
            if isinstance(q, tuple):
                cursor.executemany(*q)
            else:
                cursor.execute(q)
            try:
                results = cursor.fetchall()
            except pyodbc.ProgrammingError:
                results = None

    connection.close()

    return results


def drop_all_user_databases():
    """
    Drop all user databases. Please run the PRINT first and make sure you're not dropping anything you may regret. You may want to take backups of all databases first just in case.

    DECLARE @sql NVARCHAR(MAX) = N'';

    SELECT @sql += N'
      DROP DATABASE ' + QUOTENAME(name)
      + N';'
    FROM sys.databases
    WHERE name NOT IN (N'master',N'tempdb',N'model',N'msdb');

    PRINT @sql;
    -- EXEC master.sys.sp_executesql @sql;
    :return:
    """
    query_list = [
        # "DECLARE @sql NVARCHAR(MAX) = N'';"
        "SELECT N'DROP DATABASE ' + QUOTENAME(name) + N';' "
        "FROM sys.databases "
        "WHERE name NOT IN (N'master',N'tempdb',N'model',N'msdb',N'rdsadmin');"
        # "PRINT @sql;"
        # "EXEC master.sys.sp_executesql @sql;"
    ]

    results = mssql_cursor_context_manager(*query_list)
    query_list = [x[0] for x in results]
    if query_list:
        mssql_cursor_context_manager(*query_list)


def drop_database(db_name):
    return ["DROP DATABASE IF EXISTS {};".format(db_name)]


def create_database(db_name, collation: str = None):
    """
    CREATE DATABASE database_name
    [ CONTAINMENT = { NONE | PARTIAL } ]
    [ ON
          [ PRIMARY ] <filespec> [ ,...n ]
          [ , <filegroup> [ ,...n ] ]
          [ LOG ON <filespec> [ ,...n ] ]
    ]
    [ COLLATE collation_name ]
    [ WITH <option> [,...n ] ]
    [;]
    """
    # return_value = drop_database(db_name)
    return_value = list()
    return_value.append(
        "CREATE DATABASE {}{};".format(
            db_name,
            "" if collation is None else " COLLATE {}".format(collation))
    )

    # add user to databaseprint(query)
    return_value.extend(use_db(db_name))
    if USERNAME.upper() != "SA":
        return_value.extend(create_user(USERNAME))
    return return_value


def enable_database_tracking(db_name):
    return [
        "ALTER DATABASE {} SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)"
        .format(db_name)]


def create_user(user_name):
    """
    -- Syntax Users based on logins in master    mssql_cursor_context_manager(query)
    CREATE USER user_name
        [
            { FOR | FROM } LOGIN login_name
        ]
        [ WITH <limited_options_list> [ ,... ] ]
    [ ; ]
    """
    queries = list()
    # queries.append("CREATE USER [{0}] FOR LOGIN [{0}];".format(user_name))
    queries.append("ALTER ROLE [db_owner] ADD MEMBER [{}]".format(user_name))
    return queries


def use_db(db_name):
    """
    USE { database_name }
    [;]
    """
    return ["USE {};".format(db_name)]


def drop_schema(db_name, schema_name):
    """
    DROP SCHEMA  [ IF EXISTS ] schema_name
    """
    return_result = list(use_db(db_name))
    return_result.append("DROP SCHEMA IF EXISTS {};".format(schema_name))
    return return_result


def create_schema(db_name, schema_name, owner_name: str = None):
    """-- Syntax for SQL Server and Azure SQL Database

    CREATE SCHEMA schema_name_clause [ <schema_element> [ ...n ] ]

    <schema_name_clause> ::=
        {
        schema_name
        | AUTHORIZATION owner_name
        | schema_name AUTHORIZATION owner_name
        }

    <schema_element> ::=
        {
            table_definition | view_definition | grant_statement |
            revoke_statement | deny_statement
        }
    """
    # return_result = list(drop_schema(db_name, schema_name))
    return_result = list()
    return_result.append(
        "CREATE SCHEMA {}{};".format(
            schema_name,
            "" if owner_name is None else " AUTHORIZATION {}".format(owner_name))
    )

    return return_result


def drop_table(database_name, schema_name, table_name):
    """
    DROP TABLE [ IF EXISTS ] [ database_name . [ schema_name ] . | schema_name . ]
    table_name [ ,...n ]
    [ ; ]
    """
    return [
        "DROP TABLE IF EXISTS {}{}{}".format(
            "" if schema_name in (None, "public") else "{}.".format(database_name),
            "" if schema_name in (None, "public") else "{}.".format(schema_name),
            table_name)
    ]


def create_table(database_name, schema_name, table_name, columns: list,
                 primary_key=None, foreign_key=None, reference=None, tracking=False):
    """
    --Simple CREATE TABLE Syntax (common if not using options)
    CREATE TABLE
        [ database_name . [ schema_name ] . | schema_name . ] table_name
        ( { <column_definition> } [ ,...n ] )
    [ ; ]

    --Disk-Based CREATE TABLE Syntax
    CREATE TABLE
        [ database_name . [ schema_name ] . | schema_name . ] table_name
        [ AS FileTable ]
        ( {   <column_definition>
            | <computed_column_definition>
            | <column_set_definition>
            | [ <table_constraint> ] [ ,... n ]
            | [ <table_index> ] }
              [ ,...n ]
              [ PERIOD FOR SYSTEM_TIME ( system_start_time_column_name
                 , system_end_time_column_name ) ]
          )
        [ ON { partition_scheme_name ( partition_column_name )
               | filegroup
               | "default" } ]
        [ TEXTIMAGE_ON { filegroup | "default" } ]
        [ FILESTREAM_ON { partition_scheme_name
               | filegroup
               | "default" } ]
        [ WITH ( <table_option> [ ,...n ] ) ]
    [ ; ]

    <column_definition> ::=
    column_name <data_type>
        [ FILESTREAM ]
        [ COLLATE collation_name ]
        [ SPARSE ]
        [ MASKED WITH ( FUNCTION = ' mask_function ') ]
        [ CONSTRAINT constraint_name [ DEFAULT constant_expression ] ]
        [ IDENTITY [ ( seed,increment ) ]
        [ NOT FOR REPLICATION ]
        [ GENERATED ALWAYS AS ROW { START | END } [ HIDDEN ] ]
        [ NULL | NOT NULL ]
        [ ROWGUIDCOL ]
        [ ENCRYPTED WITH
            ( COLUMN_ENCRYPTION_KEY = key_name ,
              ENCRYPTION_TYPE = { DETERMINISTIC | RANDOMIZED } ,
              ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256'
            ) ]
        [ <column_constraint> [, ...n ] ]
        [ <column_index> ]

    <data type> ::=
    [ type_schema_name . ] type_name
        [ ( precision [ , scale ] | max |
            [ { CONTENT | DOCUMENT } ] xml_schema_collection ) ]

    <column_constraint> ::=
    [ CONSTRAINT constraint_name ]
    {     { PRIMARY KEY | UNIQUE }
            [ CLUSTERED | NONCLUSTERED ]
            [
                WITH FILLFACTOR = fillfactor
              | WITH ( < index_option > [ , ...n ] )
            ]
            [ ON { partition_scheme_name ( partition_column_name )
                | filegroup | "default" } ]

      | [ FOREIGN KEY ]
            REFERENCES [ schema_name . ] referenced_table_name [ ( ref_column ) ]
            [ ON DELETE { NO ACTION | CASCADE | SET NULL | SET DEFAULT } ]
            [ ON UPDATE { NO ACTION | CASCADE | SET NULL | SET DEFAULT } ]
            [ NOT FOR REPLICATION ]

      | CHECK [ NOT FOR REPLICATION ] ( logical_expression )
    }

    <column_index> ::=
     INDEX index_name [ CLUSTERED | NONCLUSTERED ]
        [ WITH ( <index_option> [ ,... n ] ) ]
        [ ON { partition_scheme_name (column_name )
             | filegroup_name
             | default
             }
        ]
        [ FILESTREAM_ON { filestream_filegroup_name | partition_scheme_name | "NULL" } ]

    <computed_column_definition> ::=
    column_name AS computed_column_expression
    [ PERSISTED [ NOT NULL ] ]
    [
        [ CONSTRAINT constraint_name ]
        { PRIMARY KEY | UNIQUE }
            [ CLUSTERED | NONCLUSTERED ]
            [
                WITH FILLFACTOR = fillfactor
              | WITH ( <index_option> [ , ...n ] )
            ]
            [ ON { partition_scheme_name ( partition_column_name )
            | filegroup | "default" } ]

        | [ FOREIGN KEY ]
            REFERENCES referenced_table_name [ ( ref_column ) ]
            [ ON DELETE { NO ACTION | CASCADE } ]
            [ ON UPDATE { NO ACTION } ]
            [ NOT FOR REPLICATION ]

        | CHECK [ NOT FOR REPLICATION ] ( logical_expression )
    ]
column_definition
    <column_set_definition> ::=
    column_set_name XML COLUMN_SET FOR ALL_SPARSE_COLUMNS

    < table_constraint > ::=
    [ CONSTRAINT constraint_name ]
    {
        { PRIMARY KEY | UNIQUE }
            [ CLUSTERED | NONCLUSTERED ]
            (column [ ASC | DESC ] [ ,...n ] )
            [
                WITH FILLFACTOR = fillfactor
               |WITH ( <index_option> [ , ...n ] )
            ]
            [ ON { partition_scheme_name (partition_column_name)
                | filegroup | "default" } ]
        | FOREIGN KEY
            ( column [ ,...n ] )
            REFERENCES referenced_table_name [ ( ref_column [ ,...n ] ) ]
            [ ON DELETE { NO ACTION | CASCADE | SET NULL | SET DEFAULT } ]
            [ ON UPDATE { NO ACTION | CASCADE | SET NULL | SET DEFAULT } ]
            [ NOT FOR REPLICATION ]
        | CHECK [ NOT FOR REPLICATION ] ( logical_expression )

    < table_index > ::=
    {
        {
          INDEX index_name [ CLUSTERED | NONCLUSTERED ]
             (column_name [ ASC | DESC ] [ ,... n ] )
        | INDEX index_name CLUSTERED COLUMNSTORE
        | INDEX index_name [ NONCLUSTERED ] COLUMNSTORE (column_name [ ,... n ] )
        }
        [ WITH ( <index_option> [ ,... n ] ) ]
        [ ON { partition_scheme_name (column_name )
             | filegroup_name
             | default
             }
        ]
        [ FILESTREAM_ON { filestream_filegroup_name | partition_scheme_name | "NULL" } ]

    }

    <table_option> ::=
    {
        [DATA_COMPRESSION = { NONE | ROW | PAGE }
          [ ON PARTITIONS ( { <partition_number_expression> | <range> }
          [ , ...n ] ) ]]
        [ FILETABLE_DIRECTORY = <directory_name> ]
        [ FILETABLE_COLLATE_FILENAME = { <collation_name> | database_default } ]
        [ FILETABLE_PRIMARY_KEY_CONSTRAINT_NAME = <constraint_name> ]
        [ FILETABLE_STREAMID_UNIQUE_CONSTRAINT_NAME = <constraint_name> ]
        [ FILETABLE_FULLPATH_UNIQUE_CONSTRAINT_NAME = <constraint_name> ]
        [ SYSTEM_VERSIONING = ON [ ( HISTORY_TABLE = schema_name . history_table_name
            [, DATA_CONSISTENCY_CHECK = { ON | OFF } ] ) ] ]
        [ REMOTE_DATA_ARCHIVE =
          {
              ON [ ( <table_stretch_options> [,...n] ) ]
            | OFF ( MIGRATION_STATE = PAUSED )
          }
        ]
    }

    <table_stretch_options> ::=
    {
        [ FILTER_PREDICATE = { null | table_predicate_function } , ]
          MIGRATION_STATE = { OUTBOUND | INBOUND | PAUSED }
     }

    <index_option> ::=
    {
        PAD_INDEX = { ON | OFF }
      | FILLFACTOR = fillfactor
      | IGNORE_DUP_KEY = { ON | OFF }
      | STATISTICS_NORECOMPUTE = { ON | OFF }
      | STATISTICS_INCREMENTAL = { ON | OFF }
      | ALLOW_ROW_LOCKS = { ON | OFF}
      | ALLOW_PAGE_LOCKS ={ ON | OFF}
      | COMPRESSION_DELAY= {0 | delay [Minutes]}
      | DATA_COMPRESSION = { NONE | ROW | PAGE | COLUMNSTORE | COLUMNSTORE_ARCHIVE }
           [ ON PARTITIONS ( { <partition_number_expression> | <range> }
           [ , ...n ] ) ]
    }
    <range> ::=
    <partition_number_expression> TO <partition_number_expression>
    """
    # return_value = drop_table(database_name, schema_name, table_name)
    return_value = list()
    column_string = ",".join(columns)
    return_value.append(
        "CREATE TABLE {}{}{} ({}{}{});".format(
            "" if schema_name in (None, "public") else "{}.".format(database_name),
            "" if schema_name in (None, "public") else "{}.".format(schema_name),
            table_name,
            column_string,
            "" if not primary_key else ", PRIMARY KEY ({})".format(
                ",".join(primary_key)),
            "" if foreign_key is None else ", FOREIGN KEY ({}) REFERENCES {}".format(
                foreign_key, reference)
            ),
        )
    if tracking:
        return_value.extend(enable_tracking_table(database_name, schema_name, table_name))
    return return_value


def enable_tracking_table(database_name, schema_name, table_name):
    return [
        "ALTER TABLE {}{}{}  ENABLE CHANGE_TRACKING".format(  # WITH (TRACK_COLUMNS_UPDATED = ON)
            "" if schema_name in (None, "public") else "{}.".format(database_name),
            "" if schema_name in (None, "public") else "{}.".format(schema_name),
            table_name)
    ]


def create_view(schema_name, view_name, select_statement):
    """
    CREATE [ OR ALTER ] VIEW [ schema_name . ] view_name [ (column [ ,...n ] ) ]
    [ WITH <view_attribute> [ ,...n ] ]
    AS select_statement
    [ WITH CHECK OPTION ]
    [ ; ]

    <view_attribute> ::=
    {
        [ ENCRYPTION ]
        [ SCHEMABINDING ]
        [ VIEW_METADATA ]
    }
    """
    return [
        "CREATE VIEW {}{} AS {};".format(
            "" if schema_name in (None, "public") else "{}.".format(schema_name),
            view_name,
            select_statement
            )
    ]


def insert(database_name, schema_name, table_name, values: list, column_names=None):
    """Values is a list of tuples to insert"""

    if not isinstance(values, list):
        raise ValueError("values needs to be a list because this is hokey")
    fields = "(" + ", ".join(['?' for _ in values[0]]) + ")"

    return [(
        "INSERT INTO {}{}{}{} VALUES {}".format(
            "" if schema_name in (None, "public") else "{}.".format(database_name),
            "" if schema_name in (None, "public") else "{}.".format(schema_name),
            table_name,
            "" if column_names is None else "({})".format(", ".join([c for c in column_names])),
            fields),
        values
    )]


def update_by_pk(database_name, schema_name, table_name, values: list, column_names: list):
    """Values is a rectangular list of tuples to update, with pk as the first value in the list"""

    if not isinstance(values, list):
        raise ValueError("values needs to be a list because this is hokey")
    fields = ", ".join(["{} = ?".format(column_names[index]) for index, _ in enumerate(values[0])])

    return_value = list()
    for row in values:
        return_value.extend([(
            "UPDATE {}{}{} SET {} WHERE {} = ?".format(
                "" if schema_name in (None, "public") else "{}.".format(database_name),
                "" if schema_name in (None, "public") else "{}.".format(schema_name),
                table_name,
                fields,
                column_names[0]),
            [row + (row[0], )]  # add pk as last value
        )])
    return return_value


def delete_by_pk(database_name, schema_name, table_name, values: list, column_names: list):
    """Values is list of tuples with the pk value to delete"""

    if not isinstance(values, list):
        raise ValueError("values needs to be a list because this is hokey")

    return_value = list()
    for row in values:
        return_value.extend([(
            "DELETE FROM {}{}{} WHERE {}".format(
                "" if schema_name in (None, "public") else "{}.".format(database_name),
                "" if schema_name in (None, "public") else "{}.".format(schema_name),
                table_name,
                " AND ".join(["{} = ?".format(column_name) for column_name in column_names])),
            [row]  # only support single column pk for now
        )])
    return return_value

def do_setup():
    database_name = "test_database3"
    schema_name = "private"

    query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
    query_list.extend(create_schema(database_name, schema_name))

    # TARGET TYPES
    #
    # ✓ means that we're covered. - means we're unsure what it maps to.
    #
    # BIGINT ✓
    # BIT ✓
    # CHAR ✓
    # DATE ✓
    # DECIMAL ✓
    # DOUBLE -
    # FLOAT ✓
    # INTEGER ✓
    # LONGVARCHAR -
    # LONGNVARCHAR -
    # NCHAR ✓
    # NVARCHAR ✓
    # NUMERIC ✓
    # REAL ✓
    # SMALLINT ✓
    # TIME ✓
    # TIMESTAMP -
    # TINYINT ✓
    # VARCHAR ✓

    #####################
    #   Numeric data    #
    #####################

    table_name = "integers"
    column_def = [
        # BIGINT
        "MyBigIntColumn bigint",
        # INTEGER
        "MyIntColumn  int",
        # SMALLINT
        "MySmallIntColumn smallint",
        # TINIINT
        "MyTinyIntColumn tinyint"
    ]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    table_name = "most_bool_columns_allowed"
    # BIT
    column_def = ["a{} bit".format(hex(x)) for x in range(1024)]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    # NUMERIC
    table_name = "numeric_precisions"
    column_def = [
        "numeric_{0}_{1} numeric({0},{1})".format(precision + 1, randint(0, precision + 1))
        for precision in range(38)
    ]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    # DECIMAL
    table_name = "decimal_precisions"
    column_def = [
        "decimal_{0}_{1} decimal({0},{1})".format(precision + 1, randint(0, precision + 1))
        for precision in range(38)
    ]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    # FLOAT
    table_name = "float_precisions"
    column_def = [
        "float_{0}_bits float({0})".format(bits + 1) for bits in range(53)
    ]
    # REAL
    column_def.append("real_24_bits real")
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    table_name = "money_money_money"
    column_def = ["cash_money money", "change smallmoney"]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    #####################
    #   Date Time Date  #
    #####################

    table_name = "dates_and_times"
    column_def = [
        # DATE
        "just_a_date date",
        # DATETIME
        "date_and_time datetime",
        "bigger_range_and_precision_datetime datetime2",
        #
        "datetime_with_timezones datetimeoffset",
        "datetime_no_seconds smalldatetime",
        # TIME
        "its_time time"
    ]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    #####################
    #   string/binary   #
    #####################

    table_name = "binary_data"
    column_def = [
        "binary_1 binary(1)",
        "binary_8000 binary(8000)"
    ]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    table_name = "varbinary_data"
    column_def = [
        "varbinary_1 varbinary(1)",
        "varbinary_8000 varbinary(8000)",
        "varbinary_max varbinary(max)"
    ]
    column_def.extend(["varbinary_{0} varbinary({0})".format(x) for x in sample(range(1, 8000), 3)])
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    # CHAR
    table_name = "char_data"
    column_def = [
        "char_1 char(1)",
        "char_8000 char(8000)"
    ]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    # VARCHAR
    table_name = "varchar_data"
    column_def = [
        "varchar_1 varchar(1)",
        "varchar_8000 varchar(8000)",
        "varchar_max varchar(max)"
    ]
    column_def.extend(["varchar_{0} varchar({0})".format(x) for x in sample(range(1, 8000), 3)])
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    # NCHAR
    table_name = "nchar_data"
    column_def = [
        "nchar_1 nchar(1)",
        "nchar_4000 nchar(4000)"
    ]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    # NVARCHAR
    table_name = "nvarchar_data"
    column_def = [
        "nvarchar_1 nvarchar(1)",
        "nvarchar_4000 nvarchar(4000)",
        "nvarchar_max nvarchar(max)"
    ]
    column_def.extend(["nvarchar_{0} nvarchar({0})".format(x) for x in sample(range(1, 4000), 3)])
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    table_name = "text_and_image_depricated_soon"
    column_def = [
        "nvarchar_text ntext",
        "varchar_text text",
        "varbinary_data image"
    ]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    #####################
    #   others          #
    #####################

    table_name = "weirdos"
    column_def = [
        "geospacial geometry",
        "geospacial_map geography",
        "markup xml",
        "guid uniqueidentifier",
        "version rowversion",
        "tree hierarchyid",
        "variant sql_variant",
        "SpecialPurposeColumns XML COLUMN_SET FOR ALL_SPARSE_COLUMNS"
        # "connection cursor"
        # "result_set table"
    ]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    table_name = "computed_columns"
    column_def = [
        "started_at datetimeoffset",
        "ended_at datetimeoffset",
        "durations_days AS DATEDIFF(day, started_at, ended_at)"
    ]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    #####################
    #   Table Names     #
    #####################

    # Regular Indentifiers
    column_def = ["pk int PRIMARY KEY"]
    object_names = [
        "#temporary_table", "a" * 128, "#" + "a" * 115, "##global_temp_table", "_underscores",
        "collation_collision", "COLLATION_COLLISION", "special_charact#r$_@middle", "hebrew_ישראל",
        "russian_самолетов", "chinese_久有归天愿", "ŝtelistoj", "[1834871389834_start_with_numbers]",
        "[~&*$!^*%^$#@&%#$_special_characters]",
        '"invalid_characters_{}"'.format("".join(OTHERS).replace('"', "")),
        '[invalid_characters_{}]'.format("".join(OTHERS).replace("[", "").replace("]", "")),
        "[tab_{}]".format(chr(9)), "[line_feed_{}]".format(chr(10)),
        "[vertical_tab_{}]".format(chr(11)), "[form_feed_{}]".format(chr(12)),
        "[return_{}]".format(chr(13))
    ]

    reserved_word = [
        "ADD", "EXTERNAL", "PROCEDURE", "ALL", "FETCH", "PUBLIC", "ALTER", "FILE", "RAISERROR", "AND",
        "FILLFACTOR", "READ", "ANY", "FOR", "READTEXT", "AS", "FOREIGN", "RECONFIGURE", "ASC",
        "FREETEXT", "REFERENCES", "AUTHORIZATION", "FREETEXTTABLE", "REPLICATION", "BACKUP", "FROM",
        "RESTORE", "BEGIN", "FULL", "RESTRICT", "BETWEEN", "FUNCTION", "RETURN", "BREAK", "GOTO",
        "REVERT", "BROWSE", "GRANT", "REVOKE", "BULK", "GROUP", "RIGHT", "BY", "HAVING", "ROLLBACK",
        "CASCADE", "HOLDLOCK", "ROWCOUNT", "CASE", "IDENTITY", "ROWGUIDCOL", "CHECK", "IDENTITY_INSERT",
        "RULE", "CHECKPOINT", "IDENTITYCOL", "SAVE", "CLOSE", "IF", "SCHEMA", "CLUSTERED", "IN",
        "SECURITYAUDIT", "COALESCE", "INDEX", "SELECT", "COLLATE", "INNER", "SEMANTICKEYPHRASETABLE",
        "COLUMN", "INSERT", "SEMANTICSIMILARITYDETAILSTABLE", "COMMIT", "INTERSECT",
        "SEMANTICSIMILARITYTABLE", "COMPUTE", "INTO", "SESSION_USER", "CONSTRAINT", "IS", "SET",
        "CONTAINS", "JOIN", "SETUSER", "CONTAINSTABLE", "KEY", "SHUTDOWN", "CONTINUE", "KILL", "SOME",
        "CONVERT", "LEFT", "STATISTICS", "CREATE", "LIKE", "SYSTEM_USER", "CROSS", "LINENO", "TABLE",
        "CURRENT", "LOAD", "TABLESAMPLE", "CURRENT_DATE", "MERGE", "TEXTSIZE", "CURRENT_TIME",
        "NATIONAL", "THEN", "CURRENT_TIMESTAMP", "NOCHECK", "TO", "CURRENT_USER", "NONCLUSTERED", "TOP",
        "CURSOR", "NOT", "TRAN", "DATABASE", "NULL", "TRANSACTION", "DBCC", "NULLIF", "TRIGGER",
        "DEALLOCATE", "OF", "TRUNCATE", "DECLARE", "OFF", "TRY_CONVERT", "DEFAULT", "OFFSETS", "TSEQUAL",
        "DELETE", "ON", "UNION", "DENY", "OPEN", "UNIQUE", "DESC", "OPENDATASOURCE", "UNPIVOT", "DISK",
        "OPENQUERY", "UPDATE", "DISTINCT", "OPENROWSET", "UPDATETEXT", "DISTRIBUTED", "OPENXML", "USE",
        "DOUBLE", "OPTION", "USER", "DROP", "OR", "VALUES", "DUMP", "ORDER", "VARYING", "ELSE", "OUTER",
        "VIEW", "END", "OVER", "WAITFOR", "ERRLVL", "PERCENT", "WHEN", "ESCAPE", "PIVOT", "WHERE",
        "EXCEPT", "PLAN", "WHILE", "EXEC", "PRECISION", "WITH", "EXECUTE", "PRIMARY", "WITHIN",
        "EXISTS", "PRINT", "WRITETEXT", "EXIT", "PROC"
    ]

    reserved_word = ['[{}]'.format(word) for word in reserved_word]
    object_names.extend(reserved_word)

    for table_name in object_names:
        query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    #####################
    #   Column Names    #
    #####################

    column_def = ["{} bit".format(col) for col in object_names]
    table_name = "column_name_test"
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    #####################
    #   Constraints     #
    #####################

    table_name = "no_constraints"
    column_def = ["column_name int"]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    table_name = "single_column_pk"
    column_def = ["pk int PRIMARY KEY", "data int"]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    table_name = "multiple_column_pk"
    column_def = [
        "first_name varchar(256)",
        "last_name varchar(256)",
        "info int"
    ]
    constraint = ["first_name", "last_name"]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                   primary_key=constraint))

    table_name = "pk_with_unique_not_null"
    column_def = ["pk int PRIMARY KEY", "data int NOT NULL UNIQUE"]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    table_name = "pk_with_fk"
    column_def = ["pk int PRIMARY KEY", "fk int"]
    foreign_key = "fk"
    reference = "{}.pk_with_unique_not_null(pk)".format(schema_name)
    query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                   foreign_key=foreign_key, reference=reference))

    table_name = "table_with_index"
    column_def = ["pk int", "data int NOT NULL INDEX myindex"]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    table_name = "default_column"
    column_def = ["pk int PRIMARY KEY", "created_at datetimeoffset DEFAULT CURRENT_TIMESTAMP"]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    table_name = "check_constraint"
    column_def = ["pk int PRIMARY KEY",
                  "birthday datetimeoffset CHECK (birthday <= CURRENT_TIMESTAMP)"]
    query_list.extend(create_table(database_name, schema_name, table_name, column_def))

    #####################
    #   Partitions      #
    #####################

    # # https://www.cathrinewilhelmsen.net/2015/04/12/table-partitioning-in-sql-server/
    # database_name = "test_partition"
    # schema_name = "partition_schema"
    #
    # query_list.extend(create_database(database_name))
    # query_list.extend(create_schema(database_name, schema_name))
    #
    # query_list.append(
    #     "CREATE PARTITION FUNCTION pfSales (DATE) "
    #     "AS RANGE RIGHT FOR VALUES "
    #     "('2013-01-01', '2014-01-01', '2015-01-01');"
    # )
    #
    # query_list.append(
    #     "CREATE PARTITION SCHEME psSales "
    #     "AS PARTITION pfSales "
    #     "ALL TO ([Primary]);"
    # )

    # the_ones_to_run = query_list[:8] + query_list[-2:]
    pprint(query_list)
    rows = mssql_cursor_context_manager(*query_list)

    # create_schema("test_database", "test_schema")
    # rows = mssql_cursor_context_manager("SELECT name FROM master.sys.databases")
    # print(rows)v


if __name__ == "__main__":
    rows = mssql_cursor_context_manager(*[
        "select * from data_types_database.dbo.char_data;"
        ])
    print(rows)
    # do_setup()
