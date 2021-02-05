"""
Test tap discovery
"""

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, create_schema

from base import BaseTapTest


class SyncMultipleFull(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_full_sync_multiple_test".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """The expected streams and metadata about the streams"""

        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""

        drop_all_user_databases()
        database_name = "database_name"
        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))

        schema_name = "schema_name"
        query_list.extend(create_schema(database_name, schema_name))

        table_name = "table_name"
        fields = [
            {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
            {'column_name': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}]
        schema = {
            'type': 'object',
            'properties': {
                'column_name': {
                    'type': ['integer', 'null'],
                    'minimum': -2147483648,
                    'maximum': 2147483647,
                    'inclusion': 'available',
                    'selected': True},
                'pk':
                    {'type': ['integer'],
                     'minimum': -2147483648,
                     'maximum': 2147483647,
                     'inclusion': 'automatic',
                     'selected': True}},
            'selected': True}

        cls.EXPECTED_METADATA = {
            '{}_{}_{}'.format(database_name, schema_name, table_name): {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': [
                    (0, 1),
                    (1, 2)],
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': table_name,
                'fields': fields,
                'schema': schema},
            }

        column_name = ["pk", "column_name"]
        column_type = ["int", "int"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        table_name = "TABLE_NAME"

        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, 3),
                (1, 4)],
            'table-key-properties': {'pk'},
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': fields,
            'schema': schema
        }

        column_name = ["pk", "column_name"]
        column_type = ["int", "int"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        schema_name = "SCHEMA_NAME"
        query_list.extend(create_schema(database_name, schema_name))

        table_name = "table_name"

        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, 5),
                (1, 6)],
            'table-key-properties': {'pk'},
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': fields,
            'schema': schema
        }

        column_name = ["pk", "column_name"]
        column_type = ["int", "int"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        table_name = "TABLE_NAME"

        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, 7),
                (1, 8)],
            'table-key-properties': {'pk'},
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': fields,
            'schema': schema
        }

        column_name = ["pk", "column_name"]
        column_type = ["int", "int"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        database_name = "DATABASE_NAME_NO_COLLISION"
        query_list.extend(create_database(database_name, "Latin1_General_CS_AS"))

        schema_name = "schema_name"
        query_list.extend(create_schema(database_name, schema_name))

        table_name = "table_name"

        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, 9),
                (1, 10)],
            'table-key-properties': {'pk'},
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': fields,
            'schema': schema
        }

        column_name = ["pk", "column_name"]
        column_type = ["int", "int"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        table_name = "TABLE_NAME"

        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, 11),
                (1, 12)],
            'table-key-properties': {'pk'},
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': fields,
            'schema': schema
        }

        column_name = ["pk", "column_name"]
        column_type = ["int", "int"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        schema_name = "SCHEMA_NAME"
        query_list.extend(create_schema(database_name, schema_name))

        table_name = "table_name"

        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, 13),
                (1, 14)],
            'table-key-properties': {'pk'},
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': fields,
            'schema': schema
        }

        column_name = ["pk", "column_name"]
        column_type = ["int", "int"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        table_name = "TABLE_NAME"

        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, 15),
                (1, 16)],
            'table-key-properties': {'pk'},
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': fields,
            'schema': schema
        }

        column_name = ["pk", "column_name"]
        column_type = ["int", "int"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        mssql_cursor_context_manager(*query_list)
        cls.expected_metadata = cls.discovery_expected_metadata

    def test_run(self):
        """
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
        additional_md = [{"breadcrumb": [], "metadata": {'replication-method': 'FULL_TABLE'}}]
        BaseTapTest.select_all_streams_and_fields(
            conn_id, found_catalogs, additional_md=additional_md)

        # clear state
        menagerie.set_state(conn_id, {})
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify record counts of streams
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys_by_stream_id())
        expected_count = {k: len(v['values']) for k, v in self.expected_metadata().items()}
        self.assertEqual(record_count_by_stream, expected_count)

        # verify records match on the first sync
        records_by_stream = runner.get_records_from_target_output()

        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                stream_expected_data = self.expected_metadata()[stream]
                table_version = records_by_stream[stream]['table_version']

                # verify on the first sync you get activate version message before and after all data
                self.assertEqual(
                    records_by_stream[stream]['messages'][0]['action'],
                    'activate_version')
                self.assertEqual(
                    records_by_stream[stream]['messages'][-1]['action'],
                    'activate_version')
                column_names = [
                    list(field_data.keys())[0] for field_data in stream_expected_data[self.FIELDS]
                ]

                expected_messages = [
                    {
                        "action": "upsert", "data":
                        {
                            column: value for column, value
                            in list(zip(column_names, stream_expected_data[self.VALUES][row]))
                        }
                    } for row in range(len(stream_expected_data[self.VALUES]))
                ]

                # remove sequences from actual values for comparison
                [message.pop("sequence") for message
                 in records_by_stream[stream]['messages'][1:-1]]

                # Verify all data is correct
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
                # TODO - change this to something for mssql once binlog (cdc) is finalized and we know what it is
                self.assertIsNone(
                    bookmark.get('lsn'),
                    msg="expected bookmark for stream to have NO lsn because we are using full-table replication")

                self.assertEqual(bookmark['version'], table_version,
                                 msg="expected bookmark for stream to match version")

                expected_schemas = self.expected_metadata()[stream]['schema']
                self.assertEqual(records_by_stream[stream]['schema'],
                                 expected_schemas,
                                 msg="expected: {} != actual: {}".format(expected_schemas,
                                                                         records_by_stream[stream]['schema']))
