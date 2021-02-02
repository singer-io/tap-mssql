"""
Test tap discovery
"""

from decimal import Decimal

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert

from base import BaseTapTest


class SyncIntFull(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_full_sync_integer_test".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """The expected streams and metadata about the streams"""

        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""

        database_name = "data_types_database"
        schema_name = "dbo"

        cls.EXPECTED_METADATA = {
            'data_types_database_dbo_integers': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': [
                    (0, -9223372036854775808, -2147483648, -32768),
                    (1, 0, 0, 0),
                    (2, 9223372036854775807, 2147483647, 32767),
                    (3, None, None, None),
                    (4, 5603121835631323156, 9665315, 11742),
                    (5, -4898597031243117659, 140946744, -16490),
                    (6, -5168593529138936444, -1746890910, 2150),
                    (7, 1331162887494168851, 1048867088, 12136),
                    (8, -4495110645908459596, -1971955745, 18257),
                    (9, -1575653240237191360, -533282078, 22022),
                    (10, 6203877631305833079, 271324086, -18782),
                    (11, 7293147954924079156, 1003163272, 3593),
                    (12, -1302715001442736465, -1626372079, 3788),
                    (13, -9062593720232233398, 1646478731, 17621)],
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': 'integers',
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'MyBigIntColumn': {'sql-datatype': 'bigint', 'selected-by-default': True,
                                        'inclusion': 'available'}},
                    {'MyIntColumn': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'MySmallIntColumn': {'sql-datatype': 'smallint', 'selected-by-default': True,
                                          'inclusion': 'available'}}],
                'schema': {
                    'type': 'object',
                    'properties': {
                        'MySmallIntColumn': {
                            'type': ['integer', 'null'],
                            'minimum': -32768,
                            'maximum': 32767,
                            'inclusion': 'available',
                            'selected': True},
                        'pk':
                            {'type': ['integer'],
                             'minimum': -2147483648,
                             'maximum': 2147483647,
                             'inclusion': 'automatic',
                             'selected': True},
                        'MyBigIntColumn': {
                            'type': ['integer', 'null'],
                            'minimum': -9223372036854775808,
                            'maximum': 9223372036854775807,
                            'inclusion': 'available',
                            'selected': True},
                        'MyIntColumn': {
                            'type': ['integer', 'null'],
                            'minimum': -2147483648,
                            'maximum': 2147483647,
                            'inclusion': 'available',
                            'selected': True}},
                    'selected': True}},
            'data_types_database_dbo_tiny_integers_and_bools': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': [
                    (0, 0, False),
                    (1, 255, True),
                    (2, None, None),
                    (3, 230, False),
                    (4, 6, True),
                    (5, 236, True),
                    (6, 27, True),
                    (7, 132, True),
                    (8, 251, False),
                    (9, 187, True),
                    (10, 157, True),
                    (11, 51, True),
                    (12, 144, True)],
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': 'tiny_integers_and_bools',
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'MyTinyIntColumn': {'sql-datatype': 'tinyint', 'selected-by-default': True,
                                         'inclusion': 'available'}},
                    {'my_boolean': {'sql-datatype': 'bit', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': {
                    'type': 'object',
                    'properties': {
                        'MyTinyIntColumn': {
                            'type': ['integer', 'null'],
                            'minimum': 0,
                            'maximum': 255,
                            'inclusion': 'available',
                            'selected': True},
                        'pk': {
                            'type': ['integer'],
                            'minimum': -2147483648,
                            'maximum': 2147483647,
                            'inclusion': 'automatic',
                            'selected': True},
                        'my_boolean': {
                            'type': ['boolean', 'null'],
                            'inclusion': 'available',
                            'selected': True}},
                    'selected': True}}}

        drop_all_user_databases()

        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))

        table_name = "integers"
        column_name = ["pk", "MyBigIntColumn", "MyIntColumn", "MySmallIntColumn"]
        column_type = ["int", "bigint", "int", "smallint"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["data_types_database_dbo_integers"]["values"]))

        table_name = "tiny_integers_and_bools"
        column_name = ["pk", "MyTinyIntColumn", "my_boolean"]
        column_type = ["int", "tinyint", "bit"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["data_types_database_dbo_tiny_integers_and_bools"]["values"]))

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
                # TODO - test schema matches expectations based on data type, nullable, not nullable, datetimes as string +, etc
                #   This needs to be consistent based on replication method so you can change replication methods
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

        # ----------------------------------------------------------------------
        # invoke the sync job AGAIN and get the same records
        # NOTE:  THIS IS ONLY DONE IN THIS TEST.  It also tests we don't send activate version before completion
        # and the talbe version is incremented
        # ----------------------------------------------------------------------
        # TODO - update the table to add a column and ensure that discovery adds the new column
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys_by_stream_id())
        expected_count = {k: len(v['values']) for k, v in self.expected_metadata().items()}
        self.assertEqual(record_count_by_stream, expected_count)
        records_by_stream = runner.get_records_from_target_output()

        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                stream_expected_data = self.expected_metadata()[stream]
                # TODO - test schema matches expectations based on data type, nullable, not nullable, datetimes as string +, etc
                #   This needs to be consistent based on replication method so you can change replication methods
                # {'action': 'upsert', 'sequence': 1560362044666000001, 'data': {'MySmallIntColumn': 0, 'pk': 1, 'MyIntColumn': 0, 'MyBigIntColumn': 0}}

                new_table_version = records_by_stream[stream]['table_version']

                # verify on a subsequent sync you get activate version message only after all data
                self.assertEqual(
                    records_by_stream[stream]['messages'][0]['action'],
                    'upsert')
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
                 in records_by_stream[stream]['messages'][0:-1]]

                # Verify all data is correct
                for expected_row, actual_row in list(
                        zip(expected_messages, records_by_stream[stream]['messages'][0:-1])):
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

                self.assertIsNone(
                    bookmark.get('lsn'),
                    msg="expected bookmark for stream to have NO lsn because we are using full-table replication")
                self.assertGreater(new_table_version, table_version,
                                   msg="table version {} didn't increate from {} on the second run".format(
                                       new_table_version,
                                       table_version))
                self.assertEqual(bookmark['version'], new_table_version,
                                 msg="expected bookmark for stream to match version")

                expected_schemas = self.expected_metadata()[stream]['schema']
                self.assertEqual(records_by_stream[stream]['schema'],
                                 expected_schemas,
                                 msg="expected: {} != actual: {}".format(expected_schemas,
                                                                         records_by_stream[stream]['schema']))
