"""
Test tap discovery
"""
from json import dumps

from tap_tester import menagerie

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager

from base import BaseTapTest


LOWER_ALPHAS, UPPER_ALPHAS, DIGITS, OTHERS = [], [], [], []
for letter in range(97, 123):
    LOWER_ALPHAS.append(chr(letter))

for letter in range(65, 91):
    UPPER_ALPHAS.append(chr(letter))

for digit in range(48, 58):
    DIGITS.append(chr(digit))

for invalid in set().union(range(32, 48), range(58, 65), range(91, 97), range(123, 127)):
    OTHERS.append(chr(invalid))

ALPHA_NUMERIC = list(set(LOWER_ALPHAS).union(set(UPPER_ALPHAS)).union(set(DIGITS)))


class DiscoveryTestNames(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_discovery_test_names".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """The expected streams and metadata about the streams"""

        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""
        drop_all_user_databases()
        database_name = "naming_convention_database"
        schema_name = "dbo"
        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))

        object_names = [
            "#temporary_table", "a" * 128, "#" + "a" * 115, "##global_temp_table", "_underscores",
            "collation_collision", "COLLATION_COLLISION", "special_charact#r$_@middle",
            "hebrew_ישראל", "russian_самолетов", "chinese_久有归天愿", "ŝtelistoj",
            "[1834871389834_start_with_numbers]",
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

        column_name = ["column_name"]
        column_type = ["int"]
        column_def = ["column_name int"]
        primary_key = set()
        for table_name in object_names:
            query_list.extend(create_table(database_name, schema_name, table_name, column_def))
            if table_name[0] == "[" and table_name[-1] == "]":
                table_name = table_name[1:-1]  # strip off brackets needed for reserved words
            if table_name[0] == '"' and table_name[-1] == '"':
                table_name = table_name[1:-1]  # strip off quotes needed for special characters
            if table_name[0] != "#":  # we are not discovering temporary tables
                cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                          column_type, primary_key)

        table_name = "column_name_test"
        column_name = ["{}".format(col) for col in object_names]
        column_type = ["bit" for _ in object_names]
        column_def = ["{} bit".format(col) for col in object_names]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def))
        # strip off brackets needed for reserved words
        column_name = [c[1:-1] if c[0] == "[" and c[-1] == "]" else c for c in column_name]
        column_name = [c[1:-1] if c[0] == '"' and c[-1] == '"' else c for c in column_name]
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        mssql_cursor_context_manager(*query_list)

        cls.expected_metadata = cls.discovery_expected_metadata

    def test_run(self):
        """
        Verify that discover creates the appropriate catalog, schema, metadata, etc.

        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • verify there is only 1 top level breadcrumb
        • verify replication key(s)
        • verify primary key(s)
        • verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • verify the actual replication matches our expected replication method
        • verify that primary, replication and foreign keys
          are given the inclusion of automatic (metadata and annotated schema).
        • verify that all other fields have inclusion of available (metadata and schema)
        """
        print("running test {}".format(self.name()))

        conn_id = self.create_connection()

        # Verify number of actual streams discovered match expected
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0,
                           msg="unable to locate schemas for connection {}".format(conn_id))
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertEqual(len(found_catalogs),
                         len(self.expected_streams()),
                         msg="Expected {} streams, actual was {} symetric difference {}".format(
                             len(self.expected_streams()),
                             len(found_catalogs),
                             set(found_catalog_names).symmetric_difference(self.expected_streams()),
                             )
                         )

        # Verify the stream names discovered were what we expect
        self.assertEqual(set(self.expected_streams()),
                         set(found_catalog_names),
                         msg="Expected streams don't match actual streams")

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        # TODO - not sure this makes sense for databases.  Howe would we handle case sensitive?
        # self.assertTrue(all([re.fullmatch(r"[a-z_]+", name) for name in found_catalog_names]),
        #                 msg="One or more streams don't follow standard naming")

        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                catalog = next(iter(
                    [catalog for catalog in found_catalogs
                     if catalog and catalog["tap_stream_id"] == stream]))

                # based on previous tests this should always be found
                assert catalog, "there is no catalog for {}".forrmat(stream)

                # verify the database and schema in the catalog
                self.assertEqual(catalog["metadata"][self.DATABASE_NAME],
                                 self.expected_metadata()[stream][self.DATABASE_NAME],
                                 msg="database-name incorrect")

                self.assertEqual(catalog["metadata"][self.SCHEMA],
                                 self.expected_metadata()[stream][self.SCHEMA],
                                 msg="schema-name incorrect")
                self.assertEqual(catalog[self.STREAM],
                                 self.expected_metadata()[stream][self.STREAM],
                                 msg="stream_name incorrect")
                # verify the primary keys
                self.assertEqual(set(catalog["metadata"][self.PRIMARY_KEYS]),
                                 set(self.expected_metadata()[stream][self.PRIMARY_KEYS]),
                                 msg="primary keys incorrect")

                # verify that nothing is selected since this is the first discovery
                self.assertTrue(all([catalog[self.SELECTED] is None,
                                     catalog["metadata"][self.SELECTED] is None]))

                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                # schema = schema_and_metadata["annotated-schema"]  # This is no longer relevant

                # verify the stream level properties are as expected
                # verify there is only 1 top level breadcrumb
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is more than one top level breadcrumb")

                # verify stream database name
                self.assertEqual(
                    stream_properties[0].get(
                        "metadata", {self.DATABASE_NAME: []}).get(self.DATABASE_NAME, []),
                    self.expected_metadata()[stream][self.DATABASE_NAME],
                    msg="expected database name {} but actual is {}".format(
                        self.expected_metadata()[stream][self.DATABASE_NAME],
                        stream_properties[0].get(
                            "metadata", {self.DATABASE_NAME: None}).get(self.DATABASE_NAME, [])))

                # verify stream schema name
                self.assertEqual(
                    stream_properties[0].get(
                        "metadata", {self.SCHEMA: []}).get(self.SCHEMA, []),
                    self.expected_metadata()[stream][self.SCHEMA],
                    msg="expected schema name {} but actual is {}".format(
                        self.expected_metadata()[stream][self.SCHEMA],
                        stream_properties[0].get(
                            "metadata", {self.SCHEMA: None}).get(self.SCHEMA, [])))

                # verify stream primary keys
                self.assertEqual(
                    set(stream_properties[0].get(
                        "metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])),
                    set(self.expected_primary_keys_by_stream_id()[stream]),
                    msg="expected primary key s{} but actual is {}".format(
                        set(self.expected_primary_keys_by_stream_id()[stream]),
                        set(stream_properties[0].get(
                            "metadata", {self.PRIMARY_KEYS: None}).get(self.PRIMARY_KEYS, []))))

                # verify stream view or table
                self.assertEqual(
                    stream_properties[0].get(
                        "metadata", {self.VIEW: []}).get(self.VIEW, []),
                    self.expected_metadata()[stream][self.VIEW],
                    msg="expected database name {} but actual is {}".format(
                        self.expected_metadata()[stream][self.VIEW],
                        stream_properties[0].get("metadata", {self.VIEW: None}).get(self.VIEW, [])))

                # verify there is no forced replication method
                self.assertTrue(stream_properties[0].get(
                    "metadata", {self.REPLICATION_METHOD: []}).get(
                    self.REPLICATION_METHOD) is None)

                field_properties = [{item['breadcrumb'][1]: item['metadata']} for item in metadata
                                    if item.get("breadcrumb")
                                    and item.get("breadcrumb")[0] == 'properties']

                for expected_field in self.expected_metadata()[stream][self.FIELDS]:
                    with self.subTest(field=expected_field):
                        actual_field = [field for field in field_properties
                                        if field.keys() == expected_field.keys()]
                        actual_field = actual_field.pop() if actual_field else dict()
                        msg = None
                        if not actual_field:
                            msg = [field.keys() for field in field_properties]
                        self.assertEqual(
                            actual_field,
                            expected_field,
                            msg="field metadata mismatches. {}".format(msg))
