"""
Test tap discovery
"""
from datetime import datetime, timedelta

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking, update_by_pk, delete_by_pk

from base import BaseTapTest


class SyncIntLogical(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_logical_sync_integer_test".format(super().name())

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
                            'selected': True},
                        "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}},
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
                            'selected': True},
                        "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}},
                    'selected': True}}}

        drop_all_user_databases()

        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        query_list.extend(enable_database_tracking(database_name))

        table_name = "integers"
        column_name = ["pk", "MyBigIntColumn", "MyIntColumn", "MySmallIntColumn"]
        column_type = ["int", "bigint", "int", "smallint"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["data_types_database_dbo_integers"]["values"]))

        table_name = "tiny_integers_and_bools"
        column_name = ["pk", "MyTinyIntColumn", "my_boolean"]
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
        additional_md = [{"breadcrumb": [], "metadata": {'replication-method': 'LOG_BASED'}}]
        BaseTapTest.select_all_streams_and_fields(
            conn_id, found_catalogs, additional_md=additional_md)

        # run a sync and verify exit codes
        record_count_by_stream = self.run_sync(conn_id, clear_state=True)

        # verify record counts of streams
        expected_count = {k: len(v['values']) for k, v in self.expected_metadata().items()}
        # self.assertEqual(record_count_by_stream, expected_count)

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
                if records_by_stream[stream]['messages'][-1].get("data"):
                    last_row_data = True
                else:
                    last_row_data = False

                self.assertEqual(
                    records_by_stream[stream]['messages'][0]['action'],
                    'activate_version')
                self.assertEqual(
                    records_by_stream[stream]['messages'][-2]['action'],
                    'activate_version')
                if last_row_data:
                    self.assertEqual(
                        records_by_stream[stream]['messages'][-3]['action'],
                        'activate_version')
                else:
                    self.assertEqual(
                        records_by_stream[stream]['messages'][-1]['action'],
                        'activate_version')
                self.assertEqual(
                    len([m for m in records_by_stream[stream]['messages'][1:] if m["action"] == "activate_version"]),
                    2,
                    msg="Expect 2 more activate version messages for end of full table and beginning of log based")

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

                # Verify all data is correct for the full table part
                if last_row_data:
                    final_row = -3
                else:
                    final_row = -2

                for expected_row, actual_row in list(
                        zip(expected_messages, records_by_stream[stream]['messages'][1:final_row])):
                    with self.subTest(expected_row=expected_row):

                        self.assertEqual(actual_row["action"], "upsert")
                        self.assertEqual(len(expected_row["data"].keys()), len(actual_row["data"].keys()),
                                         msg="there are not the same number of columns")
                        for column_name, expected_value in expected_row["data"].items():
                            self.assertEqual(expected_value, actual_row["data"][column_name],
                                             msg="expected: {} != actual {}".format(
                                                 expected_row, actual_row))

                # Verify all data is correct for the log replication part if sent
                if records_by_stream[stream]['messages'][-1].get("data"):
                    for column_name, expected_value in expected_messages[-1]["data"].items():
                        self.assertEqual(expected_value,
                                         records_by_stream[stream]['messages'][-1]["data"][column_name],
                                         msg="expected: {} != actual {}".format(
                                             expected_row, actual_row))

                print("records are correct for stream {}".format(stream))

                # verify state and bookmarks
                state = menagerie.get_state(conn_id)
                bookmark = state['bookmarks'][stream]

                self.assertIsNone(state.get('currently_syncing'), msg="expected state's currently_syncing to be None")
                self.assertIsNotNone(
                    bookmark.get('current_log_version'),
                    msg="expected bookmark to have current_log_version because we are using log replication")
                self.assertTrue(bookmark['initial_full_table_complete'], msg="expected full table to be complete")
                inital_log_version = bookmark['current_log_version']

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

        database_name = "data_types_database"
        schema_name = "dbo"
        table_name = "integers"
        column_name = ["pk", "MyBigIntColumn", "MyIntColumn", "MySmallIntColumn"]
        insert_value = [(14, 100, 100, 100)]
        update_value = [(1, 101, 101, 101)]
        delete_value = [(5, )]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = [(14, 100, 100, 100, None)]
        update_value = [(1, 101, 101, 101, None)]
        delete_value = [(5, None, None, None, datetime.utcnow())]
        self.EXPECTED_METADATA["data_types_database_dbo_integers"]["values"] = \
            insert_value + delete_value + update_value
        self.EXPECTED_METADATA["data_types_database_dbo_integers"]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )

        database_name = "data_types_database"
        schema_name = "dbo"
        table_name = "tiny_integers_and_bools"
        column_name = ["pk", "MyTinyIntColumn", "my_boolean"]
        insert_value = [(14, 100, False)]
        update_value = [(1, 101, True)]
        delete_value = [(5,)]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        insert_value = [(14, 100, False, None)]
        update_value = [(1, 101, True, None)]
        delete_value = [(5, None, None, datetime.utcnow())]
        self.EXPECTED_METADATA["data_types_database_dbo_tiny_integers_and_bools"]["values"] = \
            [self.expected_metadata()["data_types_database_dbo_tiny_integers_and_bools"]["values"][-1]] + \
            insert_value + delete_value + update_value
        self.EXPECTED_METADATA["data_types_database_dbo_tiny_integers_and_bools"]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )
        mssql_cursor_context_manager(*query_list)

        # run a sync and verify exit codes
        record_count_by_stream = self.run_sync(conn_id)
        expected_count = {k: len(v['values']) for k, v in self.expected_metadata().items()}
        self.assertEqual(record_count_by_stream, expected_count)
        records_by_stream = runner.get_records_from_target_output()

        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                stream_expected_data = self.expected_metadata()[stream]
                new_table_version = records_by_stream[stream]['table_version']

                # verify on a subsequent sync you get activate version message only after all data
                self.assertEqual(
                    records_by_stream[stream]['messages'][0]['action'],
                    'activate_version')
                self.assertTrue(all(
                    [message["action"] == "upsert" for message in records_by_stream[stream]['messages'][1:]]
                ))

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
                 in records_by_stream[stream]['messages'][1:]]

                # Verify all data is correct
                for expected_row, actual_row in list(
                        zip(expected_messages, records_by_stream[stream]['messages'][1:])):
                    with self.subTest(expected_row=expected_row):
                        self.assertEqual(actual_row["action"], "upsert")

                        # we only send the _sdc_deleted_at column for deleted rows
                        self.assertGreaterEqual(len(expected_row["data"].keys()), len(actual_row["data"].keys()),
                                         msg="there are not the same number of columns")

                        for column_name, expected_value in expected_row["data"].items():
                            if column_name != "_sdc_deleted_at":
                                self.assertEqual(expected_value, actual_row["data"][column_name],
                                                 msg="expected: {} != actual {}".format(
                                                     expected_row, actual_row))
                            elif expected_value:
                                # we have an expected value for a deleted row
                                try:
                                    actual_value = datetime.strptime(actual_row["data"][column_name],
                                                                     "%Y-%m-%dT%H:%M:%S.%fZ")
                                except ValueError:
                                    actual_value = datetime.strptime(actual_row["data"][column_name],
                                                                     "%Y-%m-%dT%H:%M:%SZ")
                                self.assertGreaterEqual(actual_value, expected_value - timedelta(seconds=15))
                                self.assertLessEqual(actual_value, expected_value + timedelta(seconds=15))
                            else:
                                # the row wasn't deleted so we can either not pass the column or it can be None
                                self.assertIsNone(actual_row["data"].get(column_name))

                print("records are correct for stream {}".format(stream))

                # verify state and bookmarks
                state = menagerie.get_state(conn_id)
                bookmark = state['bookmarks'][stream]

                self.assertIsNone(state.get('currently_syncing'), msg="expected state's currently_syncing to be None")
                self.assertIsNotNone(
                    bookmark.get('current_log_version'),
                    msg="expected bookmark to have current_log_version because we are using log replication")
                self.assertTrue(bookmark['initial_full_table_complete'], msg="expected full table to be complete")
                new_log_version = bookmark['current_log_version']
                self.assertGreater(new_log_version, inital_log_version,
                                   msg='expected log version to increase')

                self.assertEqual(bookmark['version'], table_version[stream],
                                 msg="expected bookmark for stream to match version")
                self.assertEqual(bookmark['version'], new_table_version,
                                 msg="expected bookmark for stream to match version")

                expected_schemas = self.expected_metadata()[stream]['schema']
                self.assertEqual(records_by_stream[stream]['schema'],
                                 expected_schemas,
                                 msg="expected: {} != actual: {}".format(expected_schemas,
                                                                         records_by_stream[stream]['schema']))
