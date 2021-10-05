"""
Test tap discovery
"""
from datetime import datetime, timedelta

import sys
from random import randint

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking, update_by_pk, delete_by_pk

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
CHAR_NAME = "invalid_characters_{}".format("".join(OTHERS).replace('"', ""))
VARCHAR_NAME = "1834871389834_start_with_numbers"
NVARCHAR_NAME = "hebrew_ישראל"
NCHAR_NAME = "SELECT"


class SyncNameLogical(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_logical_sync_names_test".format(super().name())

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

        # use all valid unicode characters
        chars = list(range(0, 55296))
        chars.extend(range(57344, sys.maxunicode))
        chars.reverse()  # pop starting with ascii characters

        char_values = [(pk, "".join([chr(chars.pop()) for _ in range(2)])) for pk in range(16)]
        char_schema = {
            'type': 'object',
            'selected': True,
            'properties': {
                CHAR_NAME: {
                    'type': ['string', 'null'],
                    'maxLength': 2,
                    'inclusion': 'available',
                    'selected': True},
                # 'minLength': 2},
                'pk': {
                    'maximum': 2147483647,
                    'type': ['integer'],
                    'inclusion': 'automatic',
                    'selected': True,
                    'minimum': -2147483648},
                "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}}}

        varchar_values = [
            (pk,
             chr(chars.pop()),
             "".join([chr(chars.pop()) for _ in range(15)]),
             "".join([chr(chars.pop()) for _ in range(randint(1, 16))])
             ) for pk in range(3)
        ]
        varchar_schema = {
            'type': 'object',
            'selected': True,
            'properties': {
                'pk': {
                    'maximum': 2147483647,
                    'type': ['integer'],
                    'inclusion': 'automatic',
                    'selected': True,
                    'minimum': -2147483648},
                'varchar_8000': {
                    'type': ['string', 'null'],
                    'maxLength': 8000,
                    'inclusion': 'available',
                    'selected': True},  # 'minLength': 0},
                VARCHAR_NAME: {
                    'type': ['string', 'null'],
                    'maxLength': 5,
                    'inclusion': 'available',
                    'selected': True},
                # 'minLength': 0},
                'varchar_max': {
                    'type': ['string', 'null'],
                    'maxLength': 2147483647,
                    'inclusion': 'available',
                    'selected': True},
                "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}}}
                # 'minLength': 0}}}

        nchar_values = [
            (pk,
             "".join([chr(chars.pop()) for _ in range(4)]))
            for pk in range(3)
        ]
        #  expect that values are right padded with spaces in the db.
        nchar_values = [(x, "{}{}".format(y, " " * ((16 - len(y.encode('utf-16-le'))) // 2))) for x, y in nchar_values]
        nchar_schema = {
            'type': 'object',
            'selected': True,
            'properties': {
                NCHAR_NAME: {
                    'type': ['string', 'null'],
                    'maxLength': 8,
                    'inclusion': 'available',
                    'selected': True},
                # 'minLength': 8},  # length is based on bytes, not characters
                'pk': {
                    'maximum': 2147483647,
                    'type': ['integer'],
                    'inclusion': 'automatic',
                    'selected': True,
                    'minimum': -2147483648},
                "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}}}

        chars.reverse()
        nvarchar_values = [
            (pk,
             chr(chars.pop()),
             "".join([chr(chars.pop()) for _ in range(8)]),
             "".join([chr(chars.pop()) for _ in range(randint(1, 8))])
             ) for pk in range(4)
        ]
        nvarchar_schema = {
            'type': 'object',
            'selected': True,
            'properties': {
                'nvarchar_max': {
                    'type': ['string', 'null'],
                    'maxLength': 2147483647,
                    'inclusion': 'available',
                    'selected': True},
                # 'minLength': 0},
                'pk': {
                    'maximum': 2147483647,
                    'type': ['integer'],
                    'inclusion': 'automatic',
                    'selected': True,
                    'minimum': -2147483648},
                'nvarchar_4000': {
                    'type': ['string', 'null'],
                    'maxLength': 4000,
                    'inclusion': 'available',
                    'selected': True},
                # 'minLength': 0},
                NVARCHAR_NAME: {
                    'type': ['string', 'null'],
                    'maxLength': 5,
                    'inclusion': 'available',
                    'selected': True},
                "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}}}
                # 'minLength': 0}}}

        cls.EXPECTED_METADATA = {
            'data_types_database_dbo_{}'.format(CHAR_NAME): {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': char_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': CHAR_NAME,
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {CHAR_NAME: {'sql-datatype': 'char', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': char_schema},
            'data_types_database_dbo_{}'.format(VARCHAR_NAME): {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': varchar_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': VARCHAR_NAME,
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {VARCHAR_NAME: {'sql-datatype': 'varchar', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'varchar_8000': {'sql-datatype': 'varchar', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'varchar_max': {'sql-datatype': 'varchar', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': varchar_schema},
            'data_types_database_dbo_{}'.format(NCHAR_NAME): {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': nchar_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': NCHAR_NAME,
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {NCHAR_NAME: {'sql-datatype': 'nchar', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': nchar_schema},
            'data_types_database_dbo_{}'.format(NVARCHAR_NAME): {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': nvarchar_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': NVARCHAR_NAME,
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {NVARCHAR_NAME: {'sql-datatype': 'nvarchar', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'nvarchar_4000': {'sql-datatype': 'nvarchar', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'nvarchar_max': {'sql-datatype': 'nvarchar', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': nvarchar_schema},
        }
        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        query_list.extend(enable_database_tracking(database_name))

        table_name = '"{}"'.format(CHAR_NAME)
        column_name = ["pk", table_name]  # , "char_8000"]
        column_type = ["int", "char(2)"]  # , "char(8000)"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name, char_values))

        table_name = "[{}]".format(VARCHAR_NAME)
        column_name = ["pk", table_name, "varchar_8000", "varchar_max"]
        column_type = ["int", "varchar(5)", "varchar(8000)", "varchar(max)"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name, varchar_values))

        table_name = "[{}]".format(NCHAR_NAME)
        column_name = ["pk", "[{}]".format(NCHAR_NAME)]
        column_type = ["int", "nchar(8)"]  # , "nchar(4000)"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        # strip padding off query data
        nchar_query_values = [
            (x, y.rstrip() if isinstance(y, str) else y) for x, y in nchar_values]
        query_list.extend(insert(database_name, schema_name, table_name, nchar_query_values))

        table_name = NVARCHAR_NAME
        column_name = ["pk", NVARCHAR_NAME, "nvarchar_4000", "nvarchar_max"]
        column_type = ["int", "nvarchar(5)", "nvarchar(4000)", "nvarchar(max)"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))

        query_list.extend(insert(database_name, schema_name, table_name, nvarchar_values))
        query_list.extend(['-- there are {} characters left to test'.format(len(chars))])

        cls.expected_metadata = cls.discovery_expected_metadata

        mssql_cursor_context_manager(*query_list)

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
        table_name = '"{}"'.format(CHAR_NAME)
        column_name = ["pk", table_name]
        insert_value = [(27, "10")]
        update_value = [(1, "10")]
        delete_value = [(5, )]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = [insert_value[0] + (None,)]
        update_value = [update_value[0] + (None,)]
        delete_value = [delete_value[0] + (None, datetime.utcnow())]
        self.EXPECTED_METADATA["data_types_database_dbo_{}".format(CHAR_NAME)]["values"] = \
            insert_value + delete_value + update_value
        self.EXPECTED_METADATA["data_types_database_dbo_{}".format(CHAR_NAME)]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )

        database_name = "data_types_database"
        schema_name = "dbo"
        table_name = "[{}]".format(VARCHAR_NAME)
        column_name = ["pk", table_name, "varchar_8000", "varchar_max"]
        insert_value = [(14, "10", "10", "10")]
        update_value = [(1, "10", "10", "10")]
        delete_value = [(0, )]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = [insert_value[0] + (None,)]
        update_value = [update_value[0] + (None,)]
        delete_value = [delete_value[0] + (None, None, None, datetime.utcnow())]
        self.EXPECTED_METADATA["data_types_database_dbo_{}".format(VARCHAR_NAME)]["values"] = \
            insert_value + delete_value + update_value
        self.EXPECTED_METADATA["data_types_database_dbo_{}".format(VARCHAR_NAME)]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )

        database_name = "data_types_database"
        schema_name = "dbo"
        table_name = "[{}]".format(NCHAR_NAME)
        column_name = ["pk", table_name]
        insert_value = [(14, "10101010")]
        update_value = [(1, "10101010")]
        delete_value = [(0, )]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = [insert_value[0] + (None,)]
        update_value = [update_value[0] + (None,)]
        delete_value = [delete_value[0] + (None, datetime.utcnow())]
        self.EXPECTED_METADATA["data_types_database_dbo_{}".format(NCHAR_NAME)]["values"] = \
            insert_value + delete_value + update_value
        self.EXPECTED_METADATA["data_types_database_dbo_{}".format(NCHAR_NAME)]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )

        database_name = "data_types_database"
        schema_name = "dbo"
        table_name = NVARCHAR_NAME
        column_name = ["pk", table_name, "nvarchar_4000", "nvarchar_max"]
        insert_value = [(14, "10", "10", "10")]
        update_value = [(1, "10", "10", "10")]
        delete_value = [(0,)]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = [insert_value[0] + (None,)]
        update_value = [update_value[0] + (None,)]
        delete_value = [delete_value[0] + (None, None, None, datetime.utcnow())]
        self.EXPECTED_METADATA["data_types_database_dbo_{}".format(NVARCHAR_NAME)]["values"] = \
            [self.EXPECTED_METADATA["data_types_database_dbo_{}".format(NVARCHAR_NAME)]["values"][-1]] + \
            insert_value + delete_value + update_value
        self.EXPECTED_METADATA["data_types_database_dbo_{}".format(NVARCHAR_NAME)]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )

        # run a sync and verify exit codes
        record_count_by_stream = self.run_sync(conn_id)

        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys_by_stream_id())
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
