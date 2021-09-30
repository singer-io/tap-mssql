"""
Test tap discovery
"""
from decimal import getcontext, Decimal

import simplejson

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking, update_by_pk, delete_by_pk

from base import BaseTapTest

getcontext().prec = 38
DECIMAL_PRECISION_SCALE = [(9, 4), (19, 6), (28, 6), (38, 13)]
NUMERIC_PRECISION_SCALE = [(9, 4), (19, 12), (28, 22), (38, 3)]


class SyncDecimalIncremental(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_incremental_sync_decimal_test".format(super().name())

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

        numeric_values = [
            (0, Decimal('-99999.9999'), Decimal('-9999999.999999999999'), Decimal('-999999.9999999999999999999999'), Decimal('-99999999999999999999999999999999999.999')),
            (1, 0, 0, 0, 0),
            (2, None, None, None, None),
            (3, Decimal('99999.9993'), Decimal('9999999.999999999999'), Decimal('999999.9999999999999999999999'), Decimal('99999999999999999999999999999999999.993')),
            (4, Decimal('96701.9382'), Decimal('-4371716.186100650268'), Decimal('-367352.306093776232045517794'), Decimal('-81147872128956247517327931319278572.985')),
            (5, Decimal('-73621.9366'), Decimal('2564047.277589545531'), Decimal('336177.4754683699464233786667'), Decimal('46946462608534127558389411015159825.758')),
            (6, Decimal('-3070.7339'), Decimal('6260062.158440967433'), Decimal('-987006.0035971607740533206418'), Decimal('95478671259010046866787754969592794.61'))]

        numeric_schema = {
            'type': 'object',
            'properties': {
                'numeric_9_4': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': 0.0001,
                    'maximum': 1e5,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e5},
                'numeric_19_12': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': 1e-12,
                    'maximum': 1e7,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e7},
                'numeric_28_22': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': 1e-22,
                    'maximum': 1e6,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e6},
                'replication_key_column': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': .001,
                    'maximum': 1e35,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e35},
                'pk': {
                    'maximum': 2147483647,
                    'type': ['integer'],
                    'inclusion': 'automatic',
                    'minimum': -2147483648,
                    'selected': True}},
            'selected': True}

        decimal_values = [
            (0, Decimal('-99999.9999'), Decimal('-9999999999999.999999'), Decimal('-9999999999999999999999.999999'), Decimal('-9999999999999999999999999.9999999999999')),
            (1, 0, 0, 0, 0),
            (2, None, None, None, None),
            (3, Decimal('99999.9993'), Decimal('9999999999999.999999'), Decimal('9999999999999999999999.999999'), Decimal('9999999999999999999999999.9999999999993')),
            (4, Decimal('-92473.8401'), Decimal('-4182159664734.645653'), Decimal('6101329656084900380190.268036'), Decimal('4778017533841887320066645.9761464001349')),
            (5, Decimal('-57970.8157'), Decimal('7735958802279.086687'), Decimal('4848737828398517845540.057905'), Decimal('2176036096567853905237453.5152648989022')),
            (6, Decimal('57573.9037'), Decimal('5948502499261.181557'), Decimal('-6687721783088280707003.076638'), Decimal('-6264019242578746090842245.3746225058202'))]

        decimal_schema = {
            'type': 'object',
            'properties': {
                'decimal_9_4': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': 0.0001,
                    'maximum': 1e5,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e5},
                'decimal_19_6': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': 1e-6,
                    'maximum': 1e13,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e13},
                'decimal_28_6': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': 1e-6,
                    'maximum': 1e22,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e22},
                'replication_key_column': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': 1e-13,
                    'maximum': 1e25,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e25},
                'pk': {
                    'maximum': 2147483647,
                    'type': ['integer'],
                    'inclusion': 'automatic',
                    'minimum': -2147483648,
                    'selected': True}},
            'selected': True}

        cls.EXPECTED_METADATA = {
            'data_types_database_dbo_numeric_precisions': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': numeric_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': 'numeric_precisions',
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'numeric_9_4': {'sql-datatype': 'numeric(9,4)', 'selected-by-default': True,
                                                'inclusion': 'available'}},
                    {'numeric_19_12': {'sql-datatype': 'numeric(19,12)', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'numeric_28_22': {'sql-datatype': 'numeric(28,22)', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'replication_key_column': {'sql-datatype': 'numeric(38,3)', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': numeric_schema},
            'data_types_database_dbo_decimal_precisions': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': decimal_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': 'decimal_precisions',
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'decimal_9_4': {'sql-datatype': 'decimal(9,4)', 'selected-by-default': True,
                                                'inclusion': 'available'}},
                    {'decimal_19_6': {'sql-datatype': 'decimal(19,6)', 'selected-by-default': True,
                                      'inclusion': 'available'}},
                    {'decimal_28_6': {'sql-datatype': 'decimal(28,6)', 'selected-by-default': True,
                                       'inclusion': 'available'}},
                    {'replication_key_column': {'sql-datatype': 'decimal(38,13)', 'selected-by-default': True,
                                      'inclusion': 'available'}}],
                'schema': decimal_schema}}
        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        query_list.extend(enable_database_tracking(database_name))

        # TODO - BUG https://stitchdata.atlassian.net/browse/SRCE-1075
        table_name = "numeric_precisions"
        precision_scale = NUMERIC_PRECISION_SCALE
        column_type = [
            "numeric({},{})".format(precision, scale)
            for precision, scale in precision_scale
        ]
        column_name = ["pk"] + [x.replace("(", "_").replace(",", "_").replace(")", "")
                                for x in column_type[:-1]] + ["replication_key_column"]
        column_type = ["int"] + column_type
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["data_types_database_dbo_numeric_precisions"]["values"]))

        table_name = "decimal_precisions"
        precision_scale = DECIMAL_PRECISION_SCALE
        column_type = [
            "decimal({},{})".format(precision, scale)
            for precision, scale in precision_scale
        ]
        column_name = ["pk"] + [x.replace("(", "_").replace(",", "_").replace(")", "")
                                for x in column_type[:-1]] + ["replication_key_column"]
        column_type = ["int"] + column_type
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["data_types_database_dbo_decimal_precisions"]["values"]))

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
        BaseTapTest.select_all_streams_and_fields(
            conn_id, found_catalogs, additional_md=additional_md)

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
                self.assertEqual(records_by_stream[stream]['messages'][0]['action'], 'activate_version')
                self.assertEqual(records_by_stream[stream]['messages'][-1]['action'], 'activate_version')
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
                                               key=lambda row: (row[-1] is not None, row[-1]))
                ]

                # Verify all data is correct for incremental
                for expected_row, actual_row in list(
                        zip(expected_messages, records_by_stream[stream]['messages'][1:-1])):
                    with self.subTest(expected_row=expected_row):
                        self.assertEqual(actual_row["action"], "upsert")
                        self.assertEqual(len(expected_row["data"].keys()), len(actual_row["data"].keys()),
                                         msg="there are not the same number of columns")
                        for column_name, expected_value in expected_row["data"].items():
                            if isinstance(expected_value, Decimal):
                                self.assertEqual(type(actual_row["data"][column_name]), Decimal,
                                                 msg="decimal value is not represented as a number")
                                self.assertEqual(expected_value, actual_row["data"][column_name],
                                                 msg="expected: {} != actual {}".format(
                                                     expected_row, actual_row))
                            else:
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
                expected_bookmark = max([row[-1] for row in stream_expected_data[self.VALUES] if row[-1] is not None])
                # currently decimal replication keys aren't supported in the front end.  If they are at a later point
                # this should be a decimal comparison. https://stitchdata.atlassian.net/browse/SRCE-1331
                self.assertEqual(bookmark['replication_key_value'], float(expected_bookmark))
                # self.assertEqual(bookmark['replication_key'], 'replication_key_value')

                self.assertEqual(bookmark['version'], table_version[stream],
                                 msg="expected bookmark for stream to match version")

                expected_schemas = self.expected_metadata()[stream]['schema']
                self.assertEqual(records_by_stream[stream]['schema'],
                                 simplejson.loads(simplejson.dumps(expected_schemas), use_decimal=True),
                                 msg="expected: {} != actual: {}".format(expected_schemas,
                                                                         records_by_stream[stream]['schema']))

        # ----------------------------------------------------------------------
        # invoke the sync job AGAIN and after insert, update, delete or rows
        # ----------------------------------------------------------------------

        database_name = "data_types_database"
        schema_name = "dbo"
        table_name = "numeric_precisions"
        precision_scale = NUMERIC_PRECISION_SCALE
        column_type = [
            "numeric({},{})".format(precision, scale)
            for precision, scale in precision_scale
        ]
        column_name = ["pk"] + [x.replace("(", "_").replace(",", "_").replace(")", "")
                                for x in column_type[:-1]] + ["replication_key_column"]
        insert_value = [(8, Decimal(100), Decimal(100), Decimal(100), Decimal(100)),
                        (7,
                         Decimal('99999.9995'),
                         Decimal('9999999.999999999999'),
                         Decimal('999999.9999999999999999999999'),
                         Decimal('99999999999999999999999999999999999.995'))]
        update_value = [(5, Decimal(100), Decimal(100), Decimal(100), Decimal(100)),
                        (6,
                         Decimal('99999.9999'),
                         Decimal('9999999.999999999999'),
                         Decimal('999999.9999999999999999999999'),
                         Decimal('99999999999999999999999999999999999.999'))]
        delete_value = [(4, )]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = insert_value[-1:]  # only repl_key >= gets included
        update_value = update_value[-1:]
        self.EXPECTED_METADATA["data_types_database_dbo_numeric_precisions"]["values"] = [
                (3,
                 Decimal('99999.9993'),
                 Decimal('9999999.999999999999'),
                 Decimal('999999.9999999999999999999999'),
                 Decimal('99999999999999999999999999999999999.993')),
            ] + update_value + insert_value

        database_name = "data_types_database"
        schema_name = "dbo"
        table_name = "decimal_precisions"
        precision_scale = DECIMAL_PRECISION_SCALE
        column_type = [
            "decimal({},{})".format(precision, scale)
            for precision, scale in precision_scale
        ]
        column_name = ["pk"] + [x.replace("(", "_").replace(",", "_").replace(")", "")
                                for x in column_type[:-1]] + ["replication_key_column"]
        insert_value = [(8, Decimal(100), Decimal(100), Decimal(100), Decimal(100)),
                        (7,
                         Decimal('99999.9995'),
                         Decimal('9999999999999.999999'),
                         Decimal('9999999999999999999999.999999'),
                         Decimal('9999999999999999999999999.9999999999995'))]
        update_value = [(5, Decimal(100), Decimal(100), Decimal(100), Decimal(100)),
                        (6,
                         Decimal('99999.9999'),
                         Decimal('9999999999999.999999'),
                         Decimal('9999999999999999999999.999999'),
                         Decimal('9999999999999999999999999.9999999999999'))]
        delete_value = [(4,)]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = insert_value[-1:]  # only repl_key >= gets included
        update_value = update_value[-1:]
        self.EXPECTED_METADATA["data_types_database_dbo_decimal_precisions"]["values"] = [
                (3,
                 Decimal('99999.9993'),
                 Decimal('9999999999999.999999'),
                 Decimal('9999999999999999999999.999999'),
                 Decimal('9999999999999999999999999.9999999999993'))] + update_value + insert_value

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
                                               key=lambda row: (row[-1] is not None, row[-1]))
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
                            if isinstance(expected_value, Decimal):
                                self.assertEqual(type(actual_row["data"][column_name]), Decimal,
                                                 msg="decimal value is not represented as a number")
                                self.assertEqual(expected_value, actual_row["data"][column_name],
                                                 msg="expected: {} != actual {}".format(
                                                     expected_row, actual_row))
                            else:
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
                expected_bookmark = max([row[-1] for row in stream_expected_data[self.VALUES] if row[-1] is not None])
                # currently decimal replication keys aren't supported in the front end.  If they are at a later point
                # this should be a decimal comparison. https://stitchdata.atlassian.net/browse/SRCE-1331
                self.assertEqual(bookmark['replication_key_value'], float(expected_bookmark))
                # self.assertEqual(bookmark['replication_key'], 'replication_key_value')

                self.assertEqual(bookmark['version'], table_version[stream],
                                 msg="expected bookmark for stream to match version")
                self.assertEqual(bookmark['version'], new_table_version,
                                 msg="expected bookmark for stream to match version")

                state = menagerie.get_state(conn_id)
                bookmark = state['bookmarks'][stream]

                expected_schemas = self.expected_metadata()[stream]['schema']
                self.assertEqual(records_by_stream[stream]['schema'],
                                 simplejson.loads(simplejson.dumps(expected_schemas), use_decimal=True),
                                 msg="expected: {} != actual: {}".format(expected_schemas,
                                                                         records_by_stream[stream]['schema']))
