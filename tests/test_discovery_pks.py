"""
Test tap discovery
"""
from json import dumps

from tap_tester import menagerie

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, create_view

from base import BaseTapTest


class DiscoveryTestKeys(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_discovery_test_pks".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """The expected streams and metadata about the streams"""

        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""
        drop_all_user_databases()
        database_name = "constraints_database"
        schema_name = "dbo"

        query_list = list(create_database(database_name,  "Latin1_General_CS_AS"))
        # query_list.extend(create_schema(database_name, schema_name))

        table_name = "no_constraints"
        column_name = ["column_name"]
        column_type = ["int"]
        primary_key = set()  # ["bigint"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "multiple_column_pk"
        column_name = ["first_name", "last_name", "info"]
        column_type = ["varchar(256)", "varchar(256)", "int"]
        primary_key = ["first_name", "last_name"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "single_column_pk"
        column_name = ["pk", "data"]
        column_type = ["int", "int"]
        primary_key = ["pk"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "pk_with_unique_not_null"
        column_name = ["pk", "data"]
        column_type = ["int", "int NOT NULL UNIQUE"]
        primary_key = ["pk"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        column_type = ["int", "int"]
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "pk_with_fk"
        column_name = ["pk", "fk"]
        column_type = ["int", "int"]
        primary_key = ["pk"]
        foreign_key = "fk"
        reference = "{}.pk_with_unique_not_null(pk)".format(schema_name)
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def,
            primary_key=primary_key, foreign_key=foreign_key, reference=reference))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "view_with_join"
        column_name = ["column1", "data", "column2"]
        column_type = ["int", "int", "int"]
        primary_key = []
        select = ("SELECT p.pk as column1, data, f.pk as column2 "
                  "FROM pk_with_unique_not_null p "
                  "JOIN pk_with_fk f on p.pk = f.fk")
        query_list.extend(create_view(schema_name, table_name, select))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key, view=True)

        table_name = "table_with_index"
        column_name = ["not_pk", "data"]
        column_type = ["int", "int NOT NULL INDEX myindex"]
        primary_key = []
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        column_type = ["int", "int"]
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "default_column"
        column_name = ["pk", "created_at"]
        column_type = ["int", "datetimeoffset DEFAULT CURRENT_TIMESTAMP"]
        primary_key = ["pk"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def, primary_key=primary_key))
        column_type = ["int", "datetimeoffset"]
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "check_constraint"
        column_name = ["pk", "birthday"]
        column_type = ["int", "datetimeoffset CHECK (birthday <= CURRENT_TIMESTAMP)"]
        primary_key = ["pk"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def, primary_key=primary_key))
        column_type = ["int", "datetimeoffset"]
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        # TODO add in identity to this test.
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
        self.assertEqual(len(found_catalogs),
                         len(self.expected_streams()),
                         msg="Expected {} streams, actual was {} for connection {},"
                             " actual {}".format(
                                 len(self.expected_streams()),
                                 len(found_catalogs),
                                 found_catalogs,
                                 conn_id))

        # Verify the stream names discovered were what we expect
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
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
                    [catalog for catalog in found_catalogs if catalog["tap_stream_id"] == stream]))
                assert catalog  # based on previous tests this should always be found

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
                                        if field.keys() == expected_field.keys()].pop()
                        self.assertEqual(
                            dumps(actual_field, sort_keys=True),
                            dumps(expected_field, sort_keys=True),
                            msg="field metadata mismatches")
