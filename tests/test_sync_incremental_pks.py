"""
Test tap discovery
"""
from datetime import date, datetime, timezone, time

from dateutil.tz import tzoffset

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking, update_by_pk, delete_by_pk, \
    create_view

from base import BaseTapTest


class SyncPkIncremental(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_incremental_sync_pk_test".format(super().name())

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
        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        query_list.extend(enable_database_tracking(database_name))

        table_name = "no_constraints"
        cls.EXPECTED_METADATA = {
            '{}_{}_{}'.format(database_name, schema_name, table_name): {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': [(0, ), (1, ), (2, )],
                'table-key-properties': set(),
                'selected': None,
                'database-name': database_name,
                'stream_name': table_name,
                'fields': [
                    {'replication_key_column': {'sql-datatype': 'int', 'selected-by-default': True,
                                               'inclusion': 'available'}}],
                'schema': {
                    'type': 'object',
                    'properties': {
                        'replication_key_column': {
                            'type': ['integer', 'null'],
                            'minimum': -2147483648,
                            'maximum': 2147483647,
                            'inclusion': 'available',
                            'selected': True}},
                    'selected': True}},
            }

        column_name = ["replication_key_column"]
        column_type = ["int"]
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        table_name = "multiple_column_pk"
        primary_key = ["first_name", "last_name"]
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                ("Tim", "Berners-Lee", 64),
                ("Sergey", "Brin", 45),
                ("Larry", "Page", 46)],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                    {'first_name': {'sql-datatype': 'varchar', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'last_name': {'sql-datatype': 'varchar', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'replication_key_column': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'replication_key_column': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648},
                    'first_name': {
                        'type': ['string'],
                        'maxLength': 256,
                        'inclusion': 'automatic',
                        'selected': True},  # 'minLength': 0},
                    'last_name': {
                        'type': ['string'],
                        'maxLength': 256,
                        'inclusion': 'automatic',
                        'selected': True}  # 'minLength': 0},(1, 4, 2, 5)
                    }}
        }
        column_name = ["first_name", "last_name", "replication_key_column"]
        column_type = ["varchar(256)", "varchar(256)", "int"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True) )
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        # Already covered in other tests
        # table_name = "single_column_pk"
        # primary_key = ["pk"]
        # cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
        #     'is-view': False,
        #     'schema-name': schema_name,
        #     'row-count': 0,
        #     'values': [
        #         (0, 3),
        #         (1, 4),
        #         (2, 5)],
        #     'table-key-properties': primary_key,
        #     'selected': None,
        #     'database-name': database_name,
        #     'stream_name': table_name,
        #     'fields': [
        #         {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
        #         {'replication_key_column': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
        #     'schema': {
        #         'type': 'object',
        #         'selected': True,
        #         'properties': {
        #             'pk': {
        #                 'maximum': 2147483647,
        #                 'type': ['integer'],
        #                 'inclusion': 'automatic',
        #                 'selected': True,
        #                 'minimum': -2147483648},
        #             'replication_key_column': {
        #                 'maximum': 2147483647,
        #                 'type': ['integer', 'null'],
        #                 'inclusion': 'available',
        #                 'selected': True,
        #                 'minimum': -2147483648},
        #             "_sdc_deleted_at": {'format': 'date-time', 'type': 'string'}}}
        # }
        # column_name = ["pk", "data"]
        # column_type = ["int", "int"]
        # column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        # query_list.extend(create_table(database_name, schema_name, table_name, column_def,
        #                                primary_key=primary_key, tracking=True))
        # query_list.extend(insert(database_name, schema_name, table_name,
        #                          cls.EXPECTED_METADATA['{}_{}_{}'.format(
        #                              database_name, schema_name, table_name)]["values"]))

        table_name = "pk_with_unique_not_null"
        primary_key = ["pk"]
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, 3),
                (1, 4),
                (2, 5)],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                {'replication_key_column': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'pk': {
                        'maximum': 2147483647,
                        'type': ['integer'],
                        'inclusion': 'automatic',
                        'selected': True,
                        'minimum': -2147483648},
                    'replication_key_column': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648}}}
        }
        column_name = ["pk", "replication_key_column"]
        column_type = ["int", "int NOT NULL UNIQUE"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True) )
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        table_name = "pk_with_fk"
        primary_key = ["pk"]
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (3, 1),
                (1, 0),
                (2, 0),
                (0, 1),
                (4, None)],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                {'replication_key_column': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'pk': {
                        'maximum': 2147483647,
                        'type': ['integer'],
                        'inclusion': 'automatic',
                        'selected': True,
                        'minimum': -2147483648},
                    'replication_key_column': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648}}}
        }
        column_name = ["pk", "replication_key_column"]
        column_type = ["int", "int"]
        foreign_key = "replication_key_column"
        reference = "{}.pk_with_unique_not_null(pk)".format(schema_name)
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def,
            primary_key=primary_key, foreign_key=foreign_key, reference=reference, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        table_name = "view_with_join"
        primary_key = []
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': True,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (1, 4, 0),
                (0, 3, 1),
                (0, 3, 2),
                (1, 4, 3),
                (None, None, 4)],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                {'column1': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}},
                {'data': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}},
                {'replication_key_column': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'column1': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648},
                    'data': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648},
                    'replication_key_column': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648}}}
        }
        select = ("SELECT p.pk as column1, p.replication_key_column as data, f.pk as replication_key_column "
                  "FROM pk_with_unique_not_null p "
                  "RIGHT JOIN pk_with_fk f on p.pk = f.replication_key_column")
        query_list.extend(create_view(schema_name, table_name, select))

        # This doesn't look to add value
        # table_name = "table_with_index"
        # primary_key = []
        # cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
        #     'is-view': False,
        #     'schema-name': schema_name,
        #     'row-count': 0,
        #     'values': [
        #         (0, 3),
        #         (1, 4)],
        #     'table-key-properties': primary_key,
        #     'selected': None,
        #     'database-name': database_name,
        #     'stream_name': table_name,
        #     'fields': [
        #         {'not_pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}},
        #         {'replication_key_column': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
        #     'schema': {
        #         'type': 'object',
        #         'selected': True,
        #         'properties': {
        #             'not_pk': {
        #                 'maximum': 2147483647,
        #                 'type': ['integer', 'null'],
        #                 'inclusion': 'available',
        #                 'selected': True,
        #                 'minimum': -2147483648},
        #             'replication_key_column': {
        #                 'maximum': 2147483647,
        #                 'type': ['integer', 'null'],
        #                 'inclusion': 'available',
        #                 'selected': True,
        #                 'minimum': -2147483648}}}
        # }
        # column_name = ["not_pk", "replication_key_column"]
        # column_type = ["int", "int NOT NULL INDEX myindex"]
        # column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        # query_list.extend(create_table(database_name, schema_name, table_name, column_def,
        #                                primary_key=primary_key))
        # query_list.extend(insert(database_name, schema_name, table_name,
        #                          cls.EXPECTED_METADATA['{}_{}_{}'.format(
        #                              database_name, schema_name, table_name)]["values"]))

        table_name = "default_column"
        primary_key = ["pk"]
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, ),
                (1, )],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                {'replication_key_column': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'pk': {
                        'maximum': 2147483647,
                        'type': ['integer'],
                        'inclusion': 'automatic',
                        'selected': True,
                        'minimum': -2147483648},
                    'replication_key_column': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648}}}
        }
        column_name = ["pk", "replication_key_column"]
        column_type = ["int", "int DEFAULT -1"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def, primary_key=primary_key, tracking=True) )
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"],
                                 column_names=["pk"]))
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)]["values"] = [
                (0, -1),
                (1, -1)]

        table_name = "check_constraint"
        primary_key = ["pk"]
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, 37),
                (1, 34)],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                {'replication_key_column': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'pk': {
                        'maximum': 2147483647,
                        'type': ['integer'],
                        'inclusion': 'automatic',
                        'selected': True,
                        'minimum': -2147483648},
                    'replication_key_column': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648}}}
        }
        column_name = ["pk", "replication_key_column"]
        column_type = ["int", "int CHECK (replication_key_column <= 120)"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def, primary_key=primary_key, tracking=True) )
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        table_name = "even_identity"
        primary_key = ["pk"]
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (1, ),
                (2, )],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                {'replication_key_column': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'pk': {
                        'maximum': 2147483647,
                        'type': ['integer'],
                        'inclusion': 'automatic',
                        'selected': True,
                        'minimum': -2147483648},
                    'replication_key_column': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648}}}
        }
        column_name = ["pk", "replication_key_column"]
        column_type = ["int", "int IDENTITY(2,2)"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def, primary_key=primary_key, tracking=True) )
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"],
                                 column_names=["pk"]))
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)]["values"] = [
            (1, 2),
            (2, 4)]
        mssql_cursor_context_manager(*query_list)

        cls.expected_metadata = cls.discovery_expected_metadata

    def test_run(self):
        """stream_expected_data[self.VALUES]
        Verify that a full sync can send capture all data and send it in the correct format
        for integer and boolean (bit) data.
        Verify that the fist sync sends an activate immediately.
        Verify that the table version is incremented up
        """
        print("running test {}".format(self.name()))

        conn_id = self.create_connection()

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # get the catalog information of discovery
        found_catalogs = menagerie.get_catalogs(conn_id)
        additional_md = [{"breadcrumb": [], "metadata": {'replication-method': 'INCREMENTAL',
                                                         'replication-key': 'replication_key_column'}}]

        non_selected_properties = []

        BaseTapTest.select_all_streams_and_fields(conn_id, found_catalogs, additional_md=additional_md)

        # run sync and verify exit codes
        record_count_by_stream = self.run_sync(conn_id, clear_state=True)

        # verify record counts of streams
        expected_count = {k: len(v['values']) for k, v in self.expected_metadata().items()}
        self.assertEqual(record_count_by_stream, expected_count)

        # verify records match on the first sync
        records_by_stream = runner.get_records_from_target_output()

        table_version = dict()
        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                stream_expected_data = self.expected_metadata()[stream]
                table_version[stream] = records_by_stream[stream]['table_version']

                # verify on the first sync you get
                # activate version message before and after all data for the full table
                # and before the logical replication part
                self.assertEqual(
                    records_by_stream[stream]['messages'][0]['action'],
                    'activate_version')
                self.assertEqual(
                    records_by_stream[stream]['messages'][-1]['action'],
                    'activate_version')
                self.assertTrue(
                    all([m["action"] == "upsert" for m in records_by_stream[stream]['messages'][1:-1]]),
                    msg="Expect all but the first message to be upserts")
                self.assertEqual(len(records_by_stream[stream]['messages'][1:-1]),
                                 len(stream_expected_data[self.VALUES]),
                                 msg="incorrect number of upserts")

                column_names = [
                    list(field_data.keys())[0] for field_data in stream_expected_data[self.FIELDS]
                ]
                replication_column = column_names.index("replication_key_column")
                expected_messages = [
                    {
                        "action": "upsert", "data":
                        {
                            column: value for column, value
                            in list(zip(column_names, row_values))
                            if column not in non_selected_properties
                        }
                    } for row_values in sorted(stream_expected_data[self.VALUES],
                                               key=lambda row: (row[replication_column] is not None, row[replication_column]))
                ]

                # Verify all data is correct for incremental
                for expected_row, actual_row in list(
                        zip(expected_messages, records_by_stream[stream]['messages'][1:-1])):
                    with self.subTest(expected_row=expected_row):
                        self.assertEqual(actual_row["action"], "upsert")
                        self.assertEqual(len(expected_row["data"].keys()), len(actual_row["data"].keys()),
                                         msg="there are not the same number of columns")
                        for column_name, expected_value in expected_row["data"].items():
                            self.assertEqual(expected_value, actual_row["data"][column_name],
                                             msg="expected: {} != actual {}".format(
                                                 expected_row, actual_row))
                print("records are correct for stream {}".format(stream))

                # verify state and bookmarks
                state = menagerie.get_state(conn_id)
                bookmark = state['bookmarks'][stream]

                self.assertIsNone(state.get('currently_syncing'), msg="expected state's currently_syncing to be None")
                self.assertIsNone(bookmark.get('current_log_version'), msg="no log_version for incremental")
                self.assertIsNone(bookmark.get('initial_full_table_complete'), msg="no full table for incremental")
                # find the max value of the replication key
                self.assertEqual(bookmark['replication_key_value'],
                                 max([row[replication_column] for row in stream_expected_data[self.VALUES]
                                      if row[replication_column] is not None]))
                # self.assertEqual(bookmark['replication_key'], 'replication_key_column')

                self.assertEqual(bookmark['version'], table_version[stream],
                                 msg="expected bookmark for stream to match version")

                expected_schemas = self.expected_metadata()[stream]['schema']
                self.assertEqual(records_by_stream[stream]['schema'],
                                 expected_schemas,
                                 msg="expected: {} != actual: {}".format(expected_schemas,
                                                                         records_by_stream[stream]['schema']))

        # ----------------------------------------------------------------------
        # invoke the sync job AGAIN and after insert, update, delete or rows
        # ----------------------------------------------------------------------

        database_name = "constraints_database"
        schema_name = "dbo"
        table_name = "no_constraints"
        column_name = ["replication_key_column"]
        insert_value = [(49, )]
        update_value = [(3, )]
        delete_value = [(0, )]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name))
        query_list.extend([
            "UPDATE constraints_database.dbo.no_constraints "
            "SET replication_key_column = 3 "
            "WHERE replication_key_column = 1"])
        mssql_cursor_context_manager(*query_list)
        self.EXPECTED_METADATA["constraints_database_dbo_no_constraints"]["values"] = \
            [(2, )] + insert_value + update_value

        database_name = "constraints_database"
        schema_name = "dbo"
        table_name = "multiple_column_pk"
        column_name = ["first_name", "last_name", "replication_key_column"]
        insert_value = [("Brian", "Lampkin", 72)]
        update_value = [("Sergey", "Brin", 65)]
        delete_value = [("Larry", "Page")]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:2]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        self.EXPECTED_METADATA["constraints_database_dbo_multiple_column_pk"]["values"] = \
            [("Tim", "Berners-Lee", 64)] + insert_value + update_value

        # duplicative of other testing
        # table_name = "single_column_pk"
        # column_name = ["pk", "replication_key_column"]
        # insert_value = [(3, 49)]
        # update_value = [(1, 65)]
        # delete_value = [(0,)]
        # query_list = (insert(database_name, schema_name, table_name, insert_value))
        # query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        # query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        # mssql_cursor_context_manager(*query_list)
        # insert_value = [insert_value[0] + (None,)]
        # update_value = [update_value[0] + (None,)]
        # delete_value = [delete_value[0] + (None, datetime.utcnow())]
        # self.EXPECTED_METADATA["constraints_database_dbo_single_column_pk"]["values"] = \
        #     insert_value + delete_value + update_value

        table_name = "pk_with_fk"
        column_name = ["pk", "replication_key_column"]
        insert_value = [(5, 2), (6, None)]
        delete_value = [(1,), (2,)]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        mssql_cursor_context_manager(*query_list)
        self.EXPECTED_METADATA["constraints_database_dbo_pk_with_fk"]["values"] = \
           [(0, 1), (3, 1)] + insert_value[:-1]

        table_name = "pk_with_unique_not_null"
        column_name = ["pk", "replication_key_column"]
        insert_value = [(3, 49)]
        update_value = [(1, 65)]
        delete_value = [(0,)]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        self.EXPECTED_METADATA["constraints_database_dbo_pk_with_unique_not_null"]["values"] = \
            [(2, 5)] + insert_value + update_value

        # update expected datafor VIEW_WITH_JOIN view
        self.EXPECTED_METADATA["constraints_database_dbo_view_with_join"]["values"] = \
            [(None, None, 4), (2, 5, 5), (None, None, 6)]

        table_name = "default_column"
        column_name = ["pk", "replication_key_column"]
        insert_value = [(3, 49), (4, None), (5, )]
        update_value = [(1, 65)]
        query_list = (insert(database_name, schema_name, table_name, insert_value[:2]))
        query_list.extend(insert(database_name, schema_name, table_name, insert_value[-1:], column_names=column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        self.EXPECTED_METADATA["constraints_database_dbo_default_column"]["values"] = [
                (0, -1)] + [(3, 49), (5, -1)] + update_value

        table_name = "check_constraint"
        column_name = ["pk", "replication_key_column"]
        insert_value = [(3, 49)]
        update_value = [(1, 65)]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        self.EXPECTED_METADATA["constraints_database_dbo_check_constraint"]["values"] = \
            [(0, 37)] + insert_value + update_value

        table_name = "even_identity"
        column_name = ["pk", "replication_key_column"]
        insert_value = [(3,)]
        update_value = [(2,)]
        delete_value = [(1,)]
        query_list = (insert(database_name, schema_name, table_name, insert_value, column_names=column_name[:1]))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = [insert_value[0] + (6, )]
        update_value = [update_value[0] + (4, )]
        self.EXPECTED_METADATA["constraints_database_dbo_even_identity"]["values"] = \
            insert_value + update_value

        # run sync and verify exit codes
        record_count_by_stream = self.run_sync(conn_id)
        expected_count = {k: len(v['values']) for k, v in self.expected_metadata().items()}
        self.assertEqual(record_count_by_stream, expected_count)
        records_by_stream = runner.get_records_from_target_output()

        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                stream_expected_data = self.expected_metadata()[stream]
                new_table_version = records_by_stream[stream]['table_version']

                # verify on a subsequent sync you get activate version message only after all data
                self.assertEqual(records_by_stream[stream]['messages'][0]['action'], 'activate_version')
                self.assertEqual(records_by_stream[stream]['messages'][-1]['action'], 'activate_version')
                self.assertTrue(all(
                    [message["action"] == "upsert" for message in records_by_stream[stream]['messages'][1:-1]]
                ))
                self.assertEqual(len(records_by_stream[stream]['messages'][1:-1]),
                                 len(stream_expected_data[self.VALUES]),
                                 msg="incorrect number of upserts")

                column_names = [
                    list(field_data.keys())[0] for field_data in stream_expected_data[self.FIELDS]
                ]
                replication_column = column_names.index("replication_key_column")
                expected_messages = [
                    {
                        "action": "upsert", "data":
                        {
                            column: value for column, value
                            in list(zip(column_names, row_values))
                            if column not in non_selected_properties
                        }
                    } for row_values in sorted(stream_expected_data[self.VALUES],
                                               key=lambda row: (row[replication_column] is not None, row[replication_column]))
                ]

                # remove sequences from actual values for comparison
                [message.pop("sequence") for message
                 in records_by_stream[stream]['messages'][1:-1]]

                # Verify all data is correct
                for expected_row, actual_row in list(
                        zip(expected_messages, records_by_stream[stream]['messages'][1:-1])):
                    with self.subTest(expected_row=expected_row):
                        self.assertEqual(actual_row["action"], "upsert")

                        # we only send the _sdc_deleted_at column for deleted rows
                        self.assertEqual(len(expected_row["data"].keys()), len(actual_row["data"].keys()),
                                         msg="there are not the same number of columns")
                        for column_name, expected_value in expected_row["data"].items():
                            self.assertEqual(expected_value, actual_row["data"][column_name],
                                             msg="expected: {} != actual {}".format(
                                                 expected_row, actual_row))
                        print("records are correct for stream {}".format(stream))

                # verify state and bookmarks
                state = menagerie.get_state(conn_id)
                bookmark = state['bookmarks'][stream]

                self.assertIsNone(state.get('currently_syncing'), msg="expected state's currently_syncing to be None")
                self.assertIsNone(bookmark.get('current_log_version'), msg="no log_version for incremental")
                self.assertIsNone(bookmark.get('initial_full_table_complete'), msg="no full table for incremental")
                # find the max value of the replication key
                self.assertEqual(bookmark['replication_key_value'],
                                 max([row[replication_column] for row in stream_expected_data[self.VALUES]
                                      if row[replication_column] is not None]))
                # self.assertEqual(bookmark['replication_key'], 'replication_key_column')

                self.assertEqual(bookmark['version'], table_version[stream],
                                 msg="expected bookmark for stream to match version")
                self.assertEqual(bookmark['version'], new_table_version,
                                 msg="expected bookmark for stream to match version")

                state = menagerie.get_state(conn_id)
                bookmark = state['bookmarks'][stream]

                expected_schemas = self.expected_metadata()[stream]['schema']
                self.assertEqual(records_by_stream[stream]['schema'],
                                 expected_schemas,
                                 msg="expected: {} != actual: {}".format(expected_schemas,
                                                                         records_by_stream[stream]['schema']))
