"""
Test tap discovery
"""
from datetime import date, datetime, timezone, time, timedelta
from time import strptime

from dateutil.tz import tzoffset

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking, update_by_pk, delete_by_pk

from base import BaseTapTest


class SyncDateIncremental(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_incremental_sync_date_test".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """The expected streams and metadata about the streams"""

        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""

        database_name = "data_types_database"
        schema_name = "dbo"
        drop_all_user_databases()

        values = [
            (
                0,
                date(1, 1, 1),
                datetime(1753, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(1, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(1, 1, 1, 13, 46, tzinfo=timezone(timedelta(hours=-14))).isoformat(),
                datetime(1900, 1, 1, 0, 0, tzinfo=timezone.utc),
                time(0, 0, tzinfo=timezone.utc)),
            (
                1,
                date(9999, 12, 29),
                datetime(9999, 12, 31, 23, 59, 59, 997000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 10, 14, tzinfo=timezone(timedelta(hours=14))).isoformat(),
                datetime(2079, 6, 6, 23, 59, tzinfo=timezone.utc),
                time(23, 59, 59, tzinfo=timezone.utc)),
            (2, None, None, None, None, None, None),
            (
                3,
                date(4533, 6, 9),
                datetime(3099, 2, 6, 4, 27, 37, 983000, tzinfo=timezone.utc),
                datetime(9085, 4, 30, 21, 52, 57, 492000, tzinfo=timezone.utc),
                datetime(5749, 4, 3, 1, 47, 47, 110000, tzinfo=timezone(timedelta(hours=10, minutes=5))).isoformat(),
                datetime(2031, 4, 30, 19, 32, 0, 0, tzinfo=timezone.utc),
                time(21, 9, 56, 0, tzinfo=timezone.utc)),
            (
                4,
                date(3476, 10, 14),
                datetime(7491, 4, 5, 8, 46, 0, 360000, tzinfo=timezone.utc),
                datetime(8366, 7, 13, 17, 15, 10, 102000, tzinfo=timezone.utc),
                datetime(2642, 6, 19, 21, 10, 28, 546000, tzinfo=timezone(timedelta(hours=6, minutes=15))).isoformat(),
                datetime(2024, 6, 22, 0, 36, 0, 0, tzinfo=timezone.utc),
                time(2, 14, 4, 0, tzinfo=timezone.utc))]

        schema = {
            'selected': True,
            'properties': {
                'its_time': {
                    'selected': True,
                    'inclusion': 'available',
                    'type': ['string', 'null']},
                'pk': {
                    'maximum': 2147483647,
                    'selected': True,
                    'inclusion': 'automatic',
                    'type': ['integer'],
                    'minimum': -2147483648},
                'replication_key_column': {
                    'selected': True,
                    'inclusion': 'available',
                    'type': ['string', 'null'],
                    'format': 'date-time'},
                'date_and_time': {
                    'selected': True,
                    'inclusion': 'available',
                    'type': ['string', 'null'],
                    'format': 'date-time'},
                "bigger_range_and_precision_datetime": {
                    'selected': True,
                    'inclusion': 'available',
                    'type': ['string', 'null'],
                    'format': 'date-time'},
                "datetime_with_timezones": {
                    'selected': True,
                    'inclusion': 'available',
                    'type': ['string', 'null'],
                    'format': 'date-time'},
                "datetime_no_seconds": {
                    'selected': True,
                    'inclusion': 'available',
                    'type': ['string', 'null'],
                    'format': 'date-time'}},
            'type': 'object'}

        fields = [
            {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
            {'replication_key_column': {'sql-datatype': 'date', 'selected-by-default': True, 'inclusion': 'available'}},
            {'date_and_time': {'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'available'}},
            {'bigger_range_and_precision_datetime': {'sql-datatype': 'datetime2', 'selected-by-default': True, 'inclusion': 'available'}},
            {'datetime_with_timezones': {'sql-datatype': 'datetimeoffest', 'selected-by-default': True, 'inclusion': 'available'}},
            {'datetime_no_seconds': {'sql-datatype': 'smalldatetime', 'selected-by-default': True, 'inclusion': 'available'}},
            {'its_time': {'sql-datatype': 'time', 'selected-by-default': True, 'inclusion': 'available'}}]

        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        query_list.extend(enable_database_tracking(database_name))

        table_name = "dates_and_times"
        primary_key = {"pk"}

        column_name = ["pk", "replication_key_column", "date_and_time", "bigger_range_and_precision_datetime",
                       "datetime_with_timezones", "datetime_no_seconds", "its_time"]
        column_type = ["int", "date", "datetime", "datetime2", "datetimeoffset", "smalldatetime", "time"]

        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name, values))

        mssql_cursor_context_manager(*query_list)

        values = [
            (
                0,
                date(1, 1, 1),
                datetime(1753, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(1, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(1, 1, 1, 13, 46, tzinfo=timezone(timedelta(hours=-14))).astimezone(timezone.utc),
                datetime(1900, 1, 1, 0, 0, tzinfo=timezone.utc),
                time(0, 0, tzinfo=timezone.utc)),
            (
                1,
                date(9999, 12, 29),
                datetime(9999, 12, 31, 23, 59, 59, 997000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 10, 14, tzinfo=timezone(timedelta(hours=14))).astimezone(timezone.utc),
                datetime(2079, 6, 6, 23, 59, tzinfo=timezone.utc),
                time(23, 59, 59, tzinfo=timezone.utc)),
            (2, None, None, None, None, None, None),
            (
                3,
                date(4533, 6, 9),
                datetime(3099, 2, 6, 4, 27, 37, 983000, tzinfo=timezone.utc),
                datetime(9085, 4, 30, 21, 52, 57, 492000, tzinfo=timezone.utc),
                datetime(5749, 4, 3, 1, 47, 47, 110000, tzinfo=timezone(timedelta(hours=10, minutes=5))).astimezone(timezone.utc),
                datetime(2031, 4, 30, 19, 32, 0, 0, tzinfo=timezone.utc),
                time(21, 9, 56, 0, tzinfo=timezone.utc)),
            (
                4,
                date(3476, 10, 14),
                datetime(7491, 4, 5, 8, 46, 0, 360000, tzinfo=timezone.utc),
                datetime(8366, 7, 13, 17, 15, 10, 102000, tzinfo=timezone.utc),
                datetime(2642, 6, 19, 21, 10, 28, 546000, tzinfo=timezone(timedelta(hours=6, minutes=15))).astimezone(timezone.utc),
                datetime(2024, 6, 22, 0, 36, 0, 0, tzinfo=timezone.utc),
                time(2, 14, 4, 0, tzinfo=timezone.utc))]

        cls.EXPECTED_METADATA = {
            '{}_{}_{}'.format(database_name, schema_name, table_name): {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': values,
                'table-key-properties': primary_key,
                'selected': None,
                'database-name': database_name,
                'stream_name': table_name,
                'fields': fields,
                'schema': schema}
        }

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

        BaseTapTest.select_all_streams_and_fields(conn_id, found_catalogs, additional_md=additional_md)

        # run a sync and verify exit codes
        record_count_by_stream = self.run_sync(conn_id, clear_state=True)

        # verify record counts of streams
        expected_count = {k: len(v['values']) for k, v in self.expected_metadata().items()}
        self.assertEqual(record_count_by_stream, expected_count)

        # verify records match on the first sync
        records_by_stream = runner.get_records_from_target_output()

        non_selected_properties = []

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

                expected_messages = [
                    {
                        "action": "upsert", "data":
                        {
                            column: value for column, value
                            in list(zip(column_names, row_values))
                            if column not in non_selected_properties
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
                            if isinstance(expected_value, datetime):
                                # sql server only keeps milliseconds not microseconds
                                self.assertEqual(
                                    expected_value.isoformat().replace('000+00:00', 'Z').replace('+00:00', 'Z'),
                                    actual_row["data"][column_name],
                                    msg="expected: {} != actual {}".format(
                                        expected_value.isoformat().replace('000+00:00', 'Z').replace('+00:00', 'Z'),
                                        actual_row["data"][column_name]))
                            elif isinstance(expected_value, time):
                                # sql server time has second resolution only
                                self.assertEqual(
                                    expected_value.replace(microsecond=0).isoformat().replace('+00:00', ''),
                                    actual_row["data"][column_name],
                                    msg="expected: {} != actual {}".format(
                                        expected_value.isoformat().replace('+00:00', 'Z'),
                                        actual_row["data"][column_name]))
                            elif isinstance(expected_value, date):
                                # sql server time has second resolution only
                                self.assertEqual(expected_value.isoformat() + 'T00:00:00+00:00',
                                                 actual_row["data"][column_name],
                                                 msg="expected: {} != actual {}".format(
                                                     expected_value.isoformat() + 'T00:00:00+00:00',
                                                     actual_row["data"][column_name]))
                            else:
                                self.assertEqual(expected_value, actual_row["data"][column_name],
                                                 msg="expected: {} != actual {}".format(
                                                     expected_value, actual_row["data"][column_name]))
                print("records are correct for stream {}".format(stream))

                # verify state and bookmarks
                state = menagerie.get_state(conn_id)
                bookmark = state['bookmarks'][stream]

                self.assertIsNone(state.get('currently_syncing'), msg="expected state's currently_syncing to be None")
                self.assertIsNone(bookmark.get('current_log_version'), msg="no log_version for incremental")
                self.assertIsNone(bookmark.get('initial_full_table_complete'), msg="no full table for incremental")
                # find the max value of the replication key
                expected_bookmark = max([row[1] for row in stream_expected_data[self.VALUES] if row[1] is not None])
                self.assertEqual(bookmark['replication_key_value'],
                                 expected_bookmark.isoformat())
                # self.assertEqual(bookmark['replication_key'], 'replication_key_value')

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
        table_name = "dates_and_times"
        column_name = ["pk", "replication_key_column", "date_and_time", "bigger_range_and_precision_datetime",
                       "datetime_with_timezones", "datetime_no_seconds", "its_time"]
        insert_value = [
            (
                5,
                date(9999, 12, 30),
                datetime(9999, 12, 31, 23, 59, 59, 997000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 10, 14, tzinfo=timezone(timedelta(hours=14))).isoformat(),
                datetime(2079, 6, 6, 23, 59, tzinfo=timezone.utc),
                time(23, 59, 59, tzinfo=timezone.utc)),
            (
                6,
                date(2018, 12, 29),
                datetime(9999, 12, 31, 23, 59, 59, 997000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 10, 14, tzinfo=timezone(timedelta(hours=14))).isoformat(),
                datetime(2079, 6, 6, 23, 59, tzinfo=timezone.utc),
                time(23, 59, 59, tzinfo=timezone.utc))]
        update_value = [(
                3,
                date(9999, 12, 31),
                datetime(9999, 12, 31, 23, 59, 59, 997000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 10, 14, tzinfo=timezone(timedelta(hours=10))).isoformat(),
                datetime(2079, 6, 6, 23, 59, tzinfo=timezone.utc),
                time(23, 59, 59, tzinfo=timezone.utc)),
            (
                4,
                date(2018, 12, 30),
                datetime(9999, 12, 31, 23, 59, 59, 997000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 10, 14, tzinfo=timezone(timedelta(hours=6))).isoformat(),
                datetime(2079, 6, 6, 23, 59, tzinfo=timezone.utc),
                time(23, 59, 59, tzinfo=timezone.utc))]
        delete_value = [(2, )]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)

        insert_value = [
            (
                5,
                date(9999, 12, 30),
                datetime(9999, 12, 31, 23, 59, 59, 997000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 10, 14, tzinfo=timezone(timedelta(hours=14))).astimezone(timezone.utc),
                datetime(2079, 6, 6, 23, 59, tzinfo=timezone.utc),
                time(23, 59, 59, tzinfo=timezone.utc)),
            (
                6,
                date(2018, 12, 29),
                datetime(9999, 12, 31, 23, 59, 59, 997000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 10, 14, tzinfo=timezone(timedelta(hours=14))).astimezone(timezone.utc),
                datetime(2079, 6, 6, 23, 59, tzinfo=timezone.utc),
                time(23, 59, 59, tzinfo=timezone.utc))]
        update_value = [(
                3,
                date(9999, 12, 31),
                datetime(9999, 12, 31, 23, 59, 59, 997000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 10, 14, tzinfo=timezone(timedelta(hours=10))).astimezone(timezone.utc),
                datetime(2079, 6, 6, 23, 59, tzinfo=timezone.utc),
                time(23, 59, 59, tzinfo=timezone.utc)),
            (
                4,
                date(2018, 12, 30),
                datetime(9999, 12, 31, 23, 59, 59, 997000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
                datetime(9999, 12, 31, 10, 14, tzinfo=timezone(timedelta(hours=6))).astimezone(timezone.utc),
                datetime(2079, 6, 6, 23, 59, tzinfo=timezone.utc),
                time(23, 59, 59, tzinfo=timezone.utc))]

        insert_value = insert_value[:-1]  # only repl_key >= gets included
        update_value = update_value[:-1]
        self.EXPECTED_METADATA["data_types_database_dbo_dates_and_times"]["values"] = [
              (
                  1,
                  date(9999, 12, 29),
                  datetime(9999, 12, 31, 23, 59, 59, 997000, tzinfo=timezone.utc),
                  datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
                  datetime(9999, 12, 31, 10, 14, tzinfo=timezone(timedelta(hours=14))).astimezone(timezone.utc),
                  datetime(2079, 6, 6, 23, 59, tzinfo=timezone.utc),
                  time(23, 59, 59, tzinfo=timezone.utc))
            ] + update_value + insert_value

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

                expected_messages = [
                    {
                        "action": "upsert", "data":
                        {
                            column: value for column, value
                            in list(zip(column_names, row_values))
                            if column not in non_selected_properties
                        }
                    } for row_values in sorted(stream_expected_data[self.VALUES],
                                               key=lambda row: (row[1] is not None, row[1]))
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
                            if isinstance(expected_value, datetime):
                                # sql server only keeps milliseconds not microseconds
                                self.assertEqual(
                                    expected_value.isoformat().replace('000+00:00', 'Z').replace('+00:00', 'Z'),
                                    actual_row["data"][column_name],
                                    msg="expected: {} != actual {}".format(
                                        expected_value.isoformat().replace('000+00:00', 'Z').replace('+00:00', 'Z'),
                                        actual_row["data"][column_name]))
                            elif isinstance(expected_value, time):
                                # sql server time has second resolution only
                                self.assertEqual(
                                    expected_value.replace(microsecond=0).isoformat().replace('+00:00', ''),
                                    actual_row["data"][column_name],
                                    msg="expected: {} != actual {}".format(
                                        expected_value.isoformat().replace('+00:00', 'Z'),
                                        actual_row["data"][column_name]))
                            elif isinstance(expected_value, date):
                                # sql server time has second resolution only
                                self.assertEqual(expected_value.isoformat() + 'T00:00:00+00:00',
                                                 actual_row["data"][column_name],
                                                 msg="expected: {} != actual {}".format(
                                                     expected_value.isoformat() + 'T00:00:00+00:00',
                                                     actual_row["data"][column_name]))
                            else:
                                self.assertEqual(expected_value, actual_row["data"][column_name],
                                                 msg="expected: {} != actual {}".format(
                                                     expected_value, actual_row["data"][column_name]))
                print("records are correct for stream {}".format(stream))

                # verify state and bookmarks
                state = menagerie.get_state(conn_id)
                bookmark = state['bookmarks'][stream]

                self.assertIsNone(state.get('currently_syncing'), msg="expected state's currently_syncing to be None")
                self.assertIsNone(bookmark.get('current_log_version'), msg="no log_version for incremental")
                self.assertIsNone(bookmark.get('initial_full_table_complete'), msg="no full table for incremental")
                # find the max value of the replication key
                expected_bookmark = max([row[1] for row in stream_expected_data[self.VALUES] if row[1] is not None])
                self.assertEqual(bookmark['replication_key_value'],
                                 expected_bookmark.isoformat())
                # self.assertEqual(bookmark['replication_key'], 'replication_key_value')

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
