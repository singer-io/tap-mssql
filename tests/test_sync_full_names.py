"""
Test tap discovery
"""

from random import randint

import sys

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert

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


class SyncTestNameFull(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_full_sync_names_test".format(super().name())

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
        char_name = "invalid_characters_{}".format("".join(OTHERS).replace('"', ""))
        char_schema = {
            'type': 'object',
            'selected': True,
            'properties': {
                char_name: {
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
                    'minimum': -2147483648}}}

        varchar_values = [
            (pk,
             chr(chars.pop()),
             "".join([chr(chars.pop()) for _ in range(15)]),
             "".join([chr(chars.pop()) for _ in range(randint(1, 16))])
             ) for pk in range(3)
        ]
        varchar_name = "1834871389834_start_with_numbers"
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
                varchar_name: {
                    'type': ['string', 'null'],
                    'maxLength': 5,
                    'inclusion': 'available',
                    'selected': True},
                # 'minLength': 0},
                'varchar_max': {
                    'type': ['string', 'null'],
                    'maxLength': 2147483647,
                    'inclusion': 'available',
                    'selected': True}}}
                # 'minLength': 0}}}

        nchar_values = [
            (pk,
             "".join([chr(chars.pop()) for _ in range(4)]))
            for pk in range(3)
        ]
        #  expect that values are right padded with spaces in the db.
        nchar_values = [(x, "{}{}".format(y, " " * ((16 - len(y.encode('utf-16-le'))) // 2))) for x, y in nchar_values]
        nchar_name = "SELECT"
        nchar_schema = {
            'type': 'object',
            'selected': True,
            'properties': {
                nchar_name: {
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
                    'minimum': -2147483648}}}

        chars.reverse()
        nvarchar_values = [
            (pk,
             chr(chars.pop()),
             "".join([chr(chars.pop()) for _ in range(8)]),
             "".join([chr(chars.pop()) for _ in range(randint(1, 8))])
             ) for pk in range(4)
        ]
        nvarchar_name = "hebrew_ישראל"
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
                nvarchar_name: {
                    'type': ['string', 'null'],
                    'maxLength': 5,
                    'inclusion': 'available',
                    'selected': True}}}
                # 'minLength': 0}}}

        cls.EXPECTED_METADATA = {
            'data_types_database_dbo_{}'.format(char_name): {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': char_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': char_name,
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {char_name: {'sql-datatype': 'char', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': char_schema},
            'data_types_database_dbo_{}'.format(varchar_name): {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': varchar_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': varchar_name,
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {varchar_name: {'sql-datatype': 'varchar', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'varchar_8000': {'sql-datatype': 'varchar', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'varchar_max': {'sql-datatype': 'varchar', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': varchar_schema},
            'data_types_database_dbo_{}'.format(nchar_name): {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': nchar_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': nchar_name,
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {nchar_name: {'sql-datatype': 'nchar', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': nchar_schema},
            'data_types_database_dbo_{}'.format(nvarchar_name): {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': nvarchar_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': nvarchar_name,
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {nvarchar_name: {'sql-datatype': 'nvarchar', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'nvarchar_4000': {'sql-datatype': 'nvarchar', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'nvarchar_max': {'sql-datatype': 'nvarchar', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': nvarchar_schema},
        }
        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))

        table_name = '"{}"'.format(char_name)
        column_name = ["pk", table_name]  # , "char_8000"]
        column_type = ["int", "char(2)"]  # , "char(8000)"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name, char_values))

        table_name = "[{}]".format(varchar_name)
        column_name = ["pk", table_name, "varchar_8000", "varchar_max"]
        column_type = ["int", "varchar(5)", "varchar(8000)", "varchar(max)"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name, varchar_values))

        table_name = "[{}]".format(nchar_name)
        column_name = ["pk", "[{}]".format(nchar_name)]
        column_type = ["int", "nchar(8)"]  # , "nchar(4000)"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        # strip padding off query data
        nchar_query_values = [
            (x, y.rstrip() if isinstance(y, str) else y) for x, y in nchar_values]
        query_list.extend(insert(database_name, schema_name, table_name, nchar_query_values))

        table_name = nvarchar_name
        column_name = ["pk", nvarchar_name, "nvarchar_4000", "nvarchar_max"]
        column_type = ["int", "nvarchar(5)", "nvarchar(4000)", "nvarchar(max)"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))

        query_list.extend(insert(database_name, schema_name, table_name, nvarchar_values))
        query_list.extend(['-- there are {} characters left to test'.format(len(chars))])

        cls.expected_metadata = cls.discovery_expected_metadata

        mssql_cursor_context_manager(*query_list)

    def test_run(self):
        """
        Verify that a full sync can send capture all data and send it in the correct format
        """
        print("running test {}".format(self.name()))

        conn_id = self.create_connection()

        self.maxDiff = None

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # get the catalog information of discovery
        found_catalogs = menagerie.get_catalogs(conn_id)
        additional_md = [{"breadcrumb": [], "metadata": {'replication-method': 'FULL_TABLE'}}]
        BaseTapTest.select_all_streams_and_fields(
            conn_id, found_catalogs, additional_md=additional_md,
            non_selected_properties=["cash_money", "change"])

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
                            if expected_value != actual_row["data"][column_name] \
                                    and isinstance(expected_value, str) \
                                    and isinstance(actual_row["data"][column_name], str):
                                print("diff = {}".format(
                                    set(expected_value).symmetric_difference(set(actual_row["data"][column_name]))))

                            self.assertEqual(expected_value, actual_row["data"][column_name],
                                             msg="for column {} expected: {} != actual {}".format(
                                                    column_name,
                                                    expected_value,
                                                    actual_row["data"][column_name]))
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
