"""
Test tap discovery
"""

from decimal import getcontext, Decimal

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert

from base import BaseTapTest

import simplejson

getcontext().prec = 38


class SyncDecimalFull(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_full_sync_decimal_test".format(super().name())

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

        numeric_values = [
            (0, Decimal('-99999.9999'), Decimal('-9999999.999999999999'), Decimal('-999999.9999999999999999999999'), Decimal('-99999999999999999999999999999999999.999')),
            (1, 0, 0, 0, 0),
            (2, None, None, None, None),
            (3, Decimal('99999.9999'), Decimal('9999999.999999999999'), Decimal('999999.9999999999999999999999'), Decimal('99999999999999999999999999999999999.999')),
            (4, Decimal('96701.9382'), Decimal('-4371716.186100650268'), Decimal('-367352.306093776232045517794'), Decimal('-81147872128956247517327931319278572.985')),
            (5, Decimal('-73621.9366'), Decimal('2564047.277589545531'), Decimal('336177.4754683699464233786667'), Decimal('46946462608534127558389411015159825.758')),
            (6, Decimal('-3070.7339'), Decimal('6260062.158440967433'), Decimal('-987006.0035971607740533206418'), Decimal('95478671259010046866787754969592794.61'))]
        numeric_precision_scale = [(9, 4), (19, 12), (28, 22), (38, 3)]

        # TODO - Remove this workaround when we fix decimal precision and to test
        # numeric_values = [
        #     (0, Decimal('-9999999.99'), Decimal('-9999999999999.99')),
        #     (1, 0, 0),
        #     (2, None, None),
        #     (3, Decimal('9999999.99'), Decimal('9999999999999.99')),
        #     (4, Decimal('-4133076.27'), Decimal('8499042653781.28')),
        #     (5, Decimal('-8629188.35'), Decimal('-4589639716080.97')),
        #     (6, Decimal('-9444926.01'), Decimal('7151189415270.4'))]
        # numeric_precision_scale = [(9, 2), (15, 2)]
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
                'numeric_38_3': {
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
        # numeric_schema = {
        #     'type': 'object',
        #     'properties': {
        #         'numeric_9_2': {
        #             'exclusiveMaximum': True,
        #             'type': ['number', 'null'],
        #             'selected': True,
        #             'multipleOf': 0.01,
        #             'maximum': 10000000,
        #             'inclusion': 'available',
        #             'exclusiveMinimum': True,
        #             'minimum': -10000000},
        #         'numeric_15_2': {
        #             'exclusiveMaximum': True,
        #             'type': ['number', 'null'],
        #             'selected': True,
        #             'multipleOf': 0.01,
        #             'maximum': 10000000000000,
        #             'inclusion': 'available',
        #             'exclusiveMinimum': True,
        #             'minimum': -10000000000000},
        #         'pk': {
        #             'maximum': 2147483647,
        #             'type': ['integer'],
        #             'inclusion': 'automatic',
        #             'minimum': -2147483648,
        #             'selected': True}},
        #     'selected': True}

        decimal_values = [
            (0, Decimal('-99999.9999'), Decimal('-9999999999999.999999'), Decimal('-9999999999999999999999.999999'), Decimal('-9999999999999999999999999.9999999999999')),
            (1, 0, 0, 0, 0),
            (2, None, None, None, None),
            (3, Decimal('99999.9999'), Decimal('9999999999999.999999'), Decimal('9999999999999999999999.999999'), Decimal('9999999999999999999999999.9999999999999')),
            (4, Decimal('-92473.8401'), Decimal('-4182159664734.645653'), Decimal('6101329656084900380190.268036'), Decimal('4778017533841887320066645.9761464001349')),
            (5, Decimal('-57970.8157'), Decimal('7735958802279.086687'), Decimal('4848737828398517845540.057905'), Decimal('2176036096567853905237453.5152648989022')),
            (6, Decimal('57573.9037'), Decimal('5948502499261.181557'), Decimal('-6687721783088280707003.076638'), Decimal('-6264019242578746090842245.3746225058202'))]
        decimal_precision_scale = [(9, 4), (19, 6), (28, 6), (38, 13)]

        # TODO - Remove this workaround when we fix decimal precision and to test
        # decimal_values = [
        #     (0, Decimal('-9999.99999'), Decimal('-999999999999.999')),
        #     (1, 0, 0),
        #     (2, None, None),
        #     (3, Decimal('9999.99999'), Decimal('999999999999.999')),
        #     (4, Decimal('7191.0647'), Decimal('284159490729.628')),
        #     (5, Decimal('6470.19405'), Decimal('-631069143780.173')),
        #     (6, Decimal('4708.67525'), Decimal('-570692336616.609'))]
        # decimal_precision_scale = [(9, 5), (15, 3)]
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
                'decimal_38_13': {
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
        # decimal_schema = {
        #     'type': 'object',
        #     'properties': {
        #         'decimal_15_3': {
        #             'exclusiveMaximum': True,
        #             'type': ['number', 'null'],
        #             'selected': True,
        #             'multipleOf': 0.001,
        #             'maximum': 1000000000000,
        #             'inclusion': 'available',
        #             'exclusiveMinimum': True,
        #             'minimum': -1000000000000},
        #         'decimal_9_5': {
        #             'exclusiveMaximum': True,
        #             'type': ['number', 'null'],
        #             'selected': True,
        #             'multipleOf': 1e-05,
        #             'maximum': 10000,
        #             'inclusion': 'available',
        #             'exclusiveMinimum': True, 'minimum': -10000},
        #         'pk': {
        #             'maximum': 2147483647,
        #             'type': ['integer'],
        #             'inclusion': 'automatic',
        #             'minimum': -2147483648,
        #             'selected': True}},
        #     'selected': True}

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
                    {'numeric_38_3': {'sql-datatype': 'numeric(38,3)', 'selected-by-default': True, 'inclusion': 'available'}}],
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
                    {'decimal_38_13': {'sql-datatype': 'decimal(38,13)', 'selected-by-default': True,
                                      'inclusion': 'available'}}],
                'schema': decimal_schema}}
        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        # query_list.extend(create_schema(database_name, schema_name))

        # TODO - BUG https://stitchdata.atlassian.net/browse/SRCE-1075
        table_name = "numeric_precisions"
        precision_scale = numeric_precision_scale
        column_type = [
            "numeric({},{})".format(precision, scale)
            for precision, scale in precision_scale
        ]
        column_name = ["pk"] + [x.replace("(", "_").replace(",", "_").replace(")", "") for x in column_type]
        column_type = ["int"] + column_type
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["data_types_database_dbo_numeric_precisions"]["values"]))

        table_name = "decimal_precisions"
        precision_scale = decimal_precision_scale
        column_type = [
            "decimal({},{})".format(precision, scale)
            for precision, scale in precision_scale
        ]
        column_name = ["pk"] + [x.replace("(", "_").replace(",", "_").replace(")", "") for x in column_type]
        column_type = ["int"] + column_type
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["data_types_database_dbo_decimal_precisions"]["values"]))

        mssql_cursor_context_manager(*query_list)

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
                # TODO - change this to something for mssql once binlog (cdc) is finalized and we know what it is
                self.assertIsNone(
                    bookmark.get('lsn'),
                    msg="expected bookmark for stream to have NO lsn because we are using full-table replication")

                self.assertEqual(bookmark['version'], table_version,
                                 msg="expected bookmark for stream to match version")

                expected_schemas = {
                    "selected": True,
                    "type": "object",
                    "properties": {
                        k: dict(
                            **self.DATATYPE_SCHEMAS[v["sql-datatype"]],
                            selected=True,
                            inclusion=v["inclusion"]
                        )
                        for fd in stream_expected_data[self.FIELDS] for k, v in fd.items()
                    }
                }

                expected_schemas = self.expected_metadata()[stream]['schema']
                self.assertEqual(records_by_stream[stream]['schema'],
                                 simplejson.loads(simplejson.dumps(expected_schemas), use_decimal=True),
                                 msg="expected: {} != actual: {}".format(expected_schemas,
                                                                         records_by_stream[stream]['schema']))
