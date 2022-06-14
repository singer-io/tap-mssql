"""
Test table reset feature for incremental replication
"""
from datetime import datetime, timedelta
from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking, update_by_pk, delete_by_pk

from base import BaseTapTest


class IncrementalTableReset(BaseTapTest):
    """ Test table reset feature for incremental replication """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_inc_table_reset_test".format(super().name())

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
                    (2, 9223372036854775805, 2147483647, 32767),
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
                    {'replication_key_column': {'sql-datatype': 'bigint', 'selected-by-default': True,
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
                        'replication_key_column': {
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
                    (1, 253, True),
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
                    {'replication_key_column': {'sql-datatype': 'tinyint', 'selected-by-default': True,
                                         'inclusion': 'available'}},
                    {'my_boolean': {'sql-datatype': 'bit', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': {
                    'type': 'object',
                    'properties': {
                        'replication_key_column': {
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
        query_list.extend(enable_database_tracking(database_name))

        table_name = "integers"
        column_name = ["pk", "replication_key_column", "MyIntColumn", "MySmallIntColumn"]
        column_type = ["int", "bigint", "int", "smallint"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["data_types_database_dbo_integers"]["values"]))

        table_name = "tiny_integers_and_bools"
        column_name = ["pk", "replication_key_column", "my_boolean"]
        column_type = ["int", "tinyint", "bit"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["data_types_database_dbo_tiny_integers_and_bools"]["values"]))

        mssql_cursor_context_manager(*query_list)
        cls.expected_metadata = cls.discovery_expected_metadata

    def test_run(self):
        """
        Verify that the table reset feature works as expected for incremental replication.
        Simulate table reset by manipulating state to set the replication key value to None between syncs
        Verify activate_version messages for both tables across both syncs
        Verify data values for sync'd messages for both tables across both syncs
        Verify record count for both tables across both syncs
        Verify that the table version is NOT incremented between syncs
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

        self.select_all_streams_and_fields(conn_id, found_catalogs, additional_md=additional_md)

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

                # verify activate version message is present before and after all data on the first sync
                self.assertEqual(
                    records_by_stream[stream]['messages'][0]['action'],
                    'activate_version')

                self.assertTrue(
                    all([m["action"] == "upsert" for m in records_by_stream[stream]['messages'][1:-1]]),
                    msg="Expect all but the first message to be upserts")
                self.assertEqual(len(stream_expected_data[self.VALUES]),
                                 len(records_by_stream[stream]['messages'][1:-1]),
                                 msg="incorrect number of upserts")

                column_names = [
                    list(field_data.keys())[0] for field_data in stream_expected_data[self.FIELDS]
                ]

                expected_messages = [
                    {
                        "action": "upsert", "data":
                        {
                            column: value for column, value
                            in list(zip(column_names, row_values))
                        }
                    } for row_values in sorted(stream_expected_data[self.VALUES],
                                               key=lambda row: (row[1] is not None, row[1]))
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
                                 max([row[1] for row in stream_expected_data[self.VALUES] if row[1] is not None]))
                # self.assertEqual(bookmark['replication_key'], 'replication_key_value')

                self.assertEqual(bookmark['version'], table_version[stream],
                                 msg="expected bookmark for stream to match version")

                expected_schemas = self.expected_metadata()[stream]['schema']
                self.assertEqual(records_by_stream[stream]['schema'],
                                 expected_schemas,
                                 msg="expected: {} != actual: {}".format(expected_schemas,
                                                                         records_by_stream[stream]['schema']))

        # --------------------------------------------------------------------------
        # invoke the sync job AGAIN after resetting one table via state manipulation
        # --------------------------------------------------------------------------

        reset_stream = stream # order is random
        set_of_streams = self.expected_streams()
        set_of_streams.remove(reset_stream)
        non_reset_stream = " ".join(set_of_streams) # convert to string

        bookmark['replication_key_value'] = None
        menagerie.set_state(conn_id, state)

        # run sync and verify exit codes
        record_count_by_stream = self.run_sync(conn_id)
        records_by_stream = runner.get_records_from_target_output()

        reset_record_count = 1
        expected_count = {k: len(v['values']) for k, v in self.expected_metadata().items()}
        expected_count[non_reset_stream] = reset_record_count
        self.assertEqual(record_count_by_stream, expected_count)

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

                # verify state and bookmarks
                state = menagerie.get_state(conn_id)
                bookmark = state['bookmarks'][stream]

                self.assertIsNone(state.get('currently_syncing'), msg="expected state's currently_syncing to be None")
                self.assertIsNone(bookmark.get('current_log_version'), msg="no log_version for incremental")
                self.assertIsNone(bookmark.get('initial_full_table_complete'), msg="no full table for incremental")

                # find the max value of the replication key
                self.assertEqual(bookmark['replication_key_value'],
                                 max([row[1] for row in stream_expected_data[self.VALUES]
                                      if row[1] != None ]))
                self.assertEqual(bookmark['replication_key_name'], 'replication_key_column')

                self.assertEqual(bookmark['version'], table_version[stream],
                                 msg="expected bookmark for stream to match version")
                self.assertEqual(bookmark['version'], new_table_version,
                                 msg="expected bookmark for stream to match version")

                # TODO remove debug (confirms table version does not increment between syncs)
                #print(f'Table version: {table_version[stream]}, new_table_version: {new_table_version}')

                expected_schemas = self.expected_metadata()[stream]['schema']
                self.assertEqual(records_by_stream[stream]['schema'],
                                 expected_schemas,
                                 msg="expected: {} != actual: {}".format(expected_schemas,
                                                                         records_by_stream[stream]['schema']))

                column_names = [
                    list(field_data.keys())[0] for field_data in stream_expected_data[self.FIELDS]
                ]

                expected_messages = [
                    {
                        "action": "upsert", "data":
                        {
                            column: value for column, value
                            in list(zip(column_names, row_values))
                        }
                    } for row_values in sorted(stream_expected_data[self.VALUES],
                                               key=lambda row: (row[1] is not None, row[1]))
                ]

                # remove sequences from actual values for comparison
                [message.pop("sequence") for message in records_by_stream[stream]['messages'][1:-1]]

                # verify record count and data values for the stream that was NOT reset
                if stream != reset_stream:
                    messages = records_by_stream[stream]['messages']

                    # only the highest rep key value should be replicated, re-define expectations
                    if stream == 'data_types_database_dbo_integers':
                        expected_messages = list(stream_expected_data[self.VALUES][2]).sort()
                    else:
                        expected_messages = list(stream_expected_data[self.VALUES][1]).sort()

                    self.assertEqual(record_count_by_stream[stream], reset_record_count)
                    self.assertEqual(messages[1]['action'], 'upsert')
                    self.assertEqual(expected_messages, list(messages[1]['data'].values()).sort())

                    print("records are correct for stream {}".format(stream))
                    continue

                # Verify all data is correct for stream that was reset
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
