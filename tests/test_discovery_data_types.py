"""
Test tap discovery
"""
from json import dumps
from random import randint, sample

from tap_tester import menagerie

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager

from base import BaseTapTest


class DiscoveryTestDataTypes(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_discovery_test_data_types".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """The expected streams and metadata about the streams"""

        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""
        drop_all_user_databases()
        database_name = "data_types_database"
        schema_name = "dbo"

        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        # query_list.extend(create_schema(database_name, schema_name))

        table_name = "integers"
        column_name = ["MyBigIntColumn", "MyIntColumn", "MySmallIntColumn", "MyTinyIntColumn"]
        column_type = ["bigint", "int", "smallint", "tinyint"]
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "most_bool_columns_allowed"
        column_name = ["a{}".format(hex(x)) for x in range(1024)]
        column_type = ["bit" for _ in range(1024)]
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "numeric_precisions"
        column_type = [
            "numeric({0},{1})".format(precision + 1, randint(0, precision + 1))
            for precision in range(38)
        ]
        column_name = [x.replace("(", "_").replace(",", "_").replace(")", "") for x in column_type]
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "decimal_precisions"
        column_type = [
            "decimal({0},{1})".format(precision + 1, randint(0, precision + 1))
            for precision in range(38)
        ]
        column_name = [x.replace("(", "_").replace(",", "_").replace(")", "") for x in column_type]
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "float_precisions"
        column_type = ["float({0})".format(bits + 1) for bits in range(53)]
        column_name = [x.replace("(", "_").replace(",", "_").replace(")", "") for x in column_type]
        column_type.append("real")
        column_name.append("real_24_bits")
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "dates_and_times"
        column_name = ["just_a_date", "date_and_time", "bigger_range_and_precision_datetime",
                       "datetime_with_timezones", "datetime_no_seconds", "its_time"]
        column_type = ["date", "datetime", "datetime2", "datetimeoffset", "smalldatetime", "time"]
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "char_data"
        column_name = ["char_1", "char_8000"]
        column_type = ["char(1)", "char(8000)"]
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "varchar_data"
        column_name = ["varchar_1", "varchar_8000", "varchar_max"]
        column_type = ["varchar(1)", "varchar(8000)", "varchar(max)"]
        random_types = [x for x in sample(range(1, 8000), 3)]
        column_name.extend(["varchar_{0}".format(x) for x in random_types])
        column_type.extend(["varchar({0})".format(x) for x in random_types])
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "nchar_data"
        column_name = ["nchar_1", "nchar_4000"]
        column_type = ["nchar(1)", "nchar(4000)"]
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "nvarchar_data"
        column_name = ["nvarchar_1", "nvarchar_4000", "nvarchar_max"]
        column_type = ["nvarchar(1)", "nvarchar(4000)", "nvarchar(max)"]
        random_types = [x for x in sample(range(1, 4000), 3)]
        column_name.extend(["nvarchar_{0}".format(x) for x in random_types])
        column_type.extend(["nvarchar({0})".format(x) for x in random_types])
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "money_money_money"
        column_name = ["cash_money", "change"]
        column_type = ["money", "smallmoney"]
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "binary_data"
        column_name = ["binary_1", "binary_8000"]
        column_type = ["binary(1)", "binary(8000)"]
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "varbinary_data"
        column_name = ["varbinary_1", "varbinary_8000", "varbinary_max"]
        column_type = ["varbinary(1)", "varbinary(8000)", "varbinary(max)"]
        random_types = [x for x in sample(range(1, 8000), 3)]
        column_name.extend(["varbinary_{0}".format(x) for x in random_types])
        column_type.extend(["varbinary({0})".format(x) for x in random_types])
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "text_and_image_deprecated_soon"
        column_name = ["nvarchar_text", "varchar_text", "varbinary_data",
                       "rowversion_synonym_timestamp"]
        column_type = ["ntext", "text", "image", "timestamp"]
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "weirdos"
        column_name = [
            "geospacial", "geospacial_map", "markup", "guid", "version", "tree",
            "variant", "SpecialPurposeColumns"
        ]
        column_type = [
            "geometry", "geography", "xml", "uniqueidentifier", "rowversion", "hierarchyid",
            "sql_variant", "xml COLUMN_SET FOR ALL_SPARSE_COLUMNS"
        ]
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        column_type[7] = "xml"  # this is the underlying type
        cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                                  column_type, primary_key)

        table_name = "computed_columns"
        column_name = ["started_at", "ended_at", "durations_days"]
        column_type = ["datetimeoffset", "datetimeoffset", "AS DATEDIFF(day, started_at, ended_at)"]
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        column_type[2] = "int"  # this is the underlying type of a datediff
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

                # TODO - Verify that the meta-data includes valid-replication-keys
                #   (based on sql-datatypes specific to this source)
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
