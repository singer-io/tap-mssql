"""
Test tap discovery
"""

from datetime import date, datetime, timezone, time, timedelta
from dateutil.tz import tzoffset

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert

from base import BaseTapTest


class SyncDateFull(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_full_sync_datetime_test".format(super().name())

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
                date(9999, 12, 31),
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
                datetime(9085, 4, 30, 21, 52, 57, 492920, tzinfo=timezone.utc),
                datetime(5749, 4, 3, 1, 47, 47, 110809, tzinfo=timezone(timedelta(hours=10, minutes=5))).isoformat(),
                datetime(2031, 4, 30, 19, 32, tzinfo=timezone.utc),
                time(21, 9, 56, 0, tzinfo=timezone.utc)),
            (
                4,
                date(3476, 10, 14),
                datetime(7491, 4, 5, 8, 46, 0, 360000, tzinfo=timezone.utc),
                datetime(8366, 7, 13, 17, 15, 10, 102386, tzinfo=timezone.utc),
                datetime(2642, 6, 19, 21, 10, 28, 546280, tzinfo=timezone(timedelta(hours=6, minutes=15))).isoformat(),
                datetime(2024, 6, 22, 0, 36, tzinfo=timezone.utc),
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
                'just_a_date': {
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
            {'just_a_date': {'sql-datatype': 'date', 'selected-by-default': True, 'inclusion': 'available'}},
            {'date_and_time': {'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'available'}},
            {'bigger_range_and_precision_datetime': {'sql-datatype': 'datetime2', 'selected-by-default': True, 'inclusion': 'available'}},
            {'datetime_with_timezones': {'sql-datatype': 'datetimeoffest', 'selected-by-default': True, 'inclusion': 'available'}},
            {'datetime_no_seconds': {'sql-datatype': 'smalldatetime', 'selected-by-default': True, 'inclusion': 'available'}},
            {'its_time': {'sql-datatype': 'time', 'selected-by-default': True, 'inclusion': 'available'}}]

        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        # query_list.extend(create_schema(database_name, schema_name))

        table_name = "dates_and_times"
        primary_key = {"pk"}

        column_name = ["pk", "just_a_date", "date_and_time", "bigger_range_and_precision_datetime",
                       "datetime_with_timezones", "datetime_no_seconds", "its_time"]
        column_type = ["int", "date", "datetime", "datetime2", "datetimeoffset", "smalldatetime", "time"]

        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
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
                date(9999, 12, 31),
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
                datetime(9085, 4, 30, 21, 52, 57, 492920, tzinfo=timezone.utc),
                datetime(5749, 4, 3, 1, 47, 47, 110809,
                         tzinfo=timezone(timedelta(hours=10, minutes=5))).astimezone(timezone.utc),
                datetime(2031, 4, 30, 19, 32, tzinfo=timezone.utc),
                time(21, 9, 56, 0, tzinfo=timezone.utc)),
            (
                4,
                date(3476, 10, 14),
                datetime(7491, 4, 5, 8, 46, 0, 360000, tzinfo=timezone.utc),
                datetime(8366, 7, 13, 17, 15, 10, 102386, tzinfo=timezone.utc),
                datetime(2642, 6, 19, 21, 10, 28, 546280,
                         tzinfo=timezone(timedelta(hours=6, minutes=15))).astimezone(timezone.utc),
                datetime(2024, 6, 22, 0, 36, tzinfo=timezone.utc),
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
        """
        Verify that a full sync can send capture all data and send it in the correct format
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
        BaseTapTest.select_all_streams_and_fields(conn_id, found_catalogs, additional_md=additional_md)

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
                            if isinstance(expected_value, datetime):
                                # sql server only keeps milliseconds not microseconds
                                self.assertEqual(expected_value.isoformat().replace('000+00:00', 'Z').replace('+00:00', 'Z'),
                                                 actual_row["data"][column_name],
                                                 msg="expected: {} != actual {}".format(
                                                     expected_value.isoformat().replace('000+00:00', 'Z').replace('+00:00', 'Z'),
                                                     actual_row["data"][column_name]))
                            elif isinstance(expected_value, time):
                                # sql server time has second resolution only
                                self.assertEqual(expected_value.replace(microsecond=0).isoformat().replace('+00:00', ''),
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
