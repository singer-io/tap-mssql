"""
Test tap discovery
"""
import uuid
from datetime import datetime, timedelta

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking, update_by_pk, delete_by_pk

from base import BaseTapTest


class SyncOtherLogical(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_logical_sync_other_test".format(super().name())

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

        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        query_list.extend(enable_database_tracking(database_name))

        text_values = [
            (0, None, None, None),
            (1, "abc", "def", "ghi".encode('utf-8'))
        ]
        text_schema = {
            'selected': True,
            'properties': {
                'pk': {
                    'inclusion': 'automatic',
                    'maximum': 2147483647,
                    'minimum': -2147483648,
                    'type': ['integer'],
                    'selected': True},
                'rowversion_synonym_timestamp': {'inclusion': 'available', 'selected': True, 'type': ['string', 'null']},
                'varchar_text': {},
                'nvarchar_text': {},
                'varbinary_data': {},
                "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}},
            'type': 'object'}

        other_values = [
            (0, None, None, None, "827376B0-AEF4-11E9-8002-0800276BC1DF", None, None, None),
            (1, None, None, None, "ACC9A986-AEF4-11E9-8002-0800276BC1DF", None, None, None),
            (2, None, None, None, "B792681C-AEF4-11E9-8002-0800276BC1DF", None, None, None)
        ]
        other_schema = {
            'selected': True,
            'properties': {
                'markup': {},
                'variant': {},
                'geospacial': {},
                'SpecialPurposeColumns': {},
                'tree': {},
                'guid': {
                    'inclusion': 'available',
                    'selected': True,
                    'pattern': '[A-F0-9]{8}-([A-F0-9]{4}-){3}[A-F0-9]{12}',
                    'type': ['string', 'null']},
                'geospacial_map': {},
                'pk': {
                    'inclusion': 'automatic',
                    'maximum': 2147483647,
                    'minimum': -2147483648,
                    'type': ['integer'],
                    'selected': True},
                'version': {'inclusion': 'available', 'selected': True, 'type': ['string', 'null']},
                "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}},
            'type': 'object'}

        comp_values = [
            (0, datetime(1970, 7, 8, 3), datetime.now()),
            (1, datetime(1970, 1, 1, 0), datetime.now())
        ]
        comp_schema = {
            'selected': True,
            'properties': {
                'started_at': {
                    'selected': False,
                    'type': ['string', 'null'],
                    'inclusion': 'available',
                    'format': 'date-time'},
                'durations_days': {
                    'inclusion': 'available',
                    'maximum': 2147483647,
                    'minimum': -2147483648,
                    'type': ['integer', 'null'],
                    'selected': True},
                'ended_at': {
                    'format': 'date-time',
                    'inclusion': 'available',
                    'type': ['string', 'null'],
                    'selected': False},
                'pk': {
                    'inclusion': 'automatic',
                    'maximum': 2147483647,
                    'minimum': -2147483648,
                    'type': ['integer'],
                    'selected': True},
                "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}},
            'type': 'object'}

        cls.EXPECTED_METADATA = {
            'data_types_database_dbo_text_and_image_deprecated_soon': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': text_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': 'text_and_image_deprecated_soon',
                'fields': [
                    {"pk": {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {"nvarchar_text": {'sql-datatype': 'ntext', 'selected-by-default': False,
                                       'inclusion': 'unavailable'}},
                    {"varchar_text": {'sql-datatype': 'text', 'selected-by-default': False,
                                      'inclusion': 'unavailable'}},
                    {"varbinary_data": {'sql-datatype': 'image', 'selected-by-default': False,
                                        'inclusion': 'unavailable'}},
                    {"rowversion_synonym_timestamp": {'sql-datatype': 'timestamp', 'selected-by-default': True,
                                                      'inclusion': 'available'}}],
                'schema': text_schema},
            'data_types_database_dbo_weirdos': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': other_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': 'weirdos',
                'fields': [
                    {"pk": {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {"geospacial": {'sql-datatype': 'geometry', 'selected-by-default': False,
                                    'inclusion': 'unavailable'}},
                    {"geospacial_map": {'sql-datatype': 'geography', 'selected-by-default': False,
                                        'inclusion': 'unavailable'}},
                    {"markup": {'sql-datatype': 'xml', 'selected-by-default': False, 'inclusion': 'unavailable'}},
                    {"guid": {'sql-datatype': 'uniqueidentifier', 'selected-by-default': True,
                              'inclusion': 'available'}},
                    {"tree": {'sql-datatype': 'hierarchyid', 'selected-by-default': False, 'inclusion': 'unavailable'}},
                    {"variant": {'sql-datatype': 'sql_variant', 'selected-by-default': False,
                                 'inclusion': 'unavailable'}},
                    {"SpecialPurposeColumns": {'sql-datatype': 'xml', 'selected-by-default': False,
                                               'inclusion': 'unavailable'}},
                    {"version": {'sql-datatype': 'timestamp', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': other_schema},
            'data_types_database_dbo_computed_columns': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': comp_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': 'computed_columns',
                'fields': [
                    {"pk": {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {"started_at": {'sql-datatype': 'datetimeoffset', 'selected-by-default': True,
                                    'inclusion': 'available'}},
                    {"ended_at": {'sql-datatype': 'datetimeoffset', 'selected-by-default': True,
                                  'inclusion': 'available'}},
                    {"durations_days": {'sql-datatype': 'int', 'selected-by-default': True,
                                        'inclusion': 'unavailable'}}],
                'schema': comp_schema},
        }

        # test timestamp and usnupported
        table_name = "text_and_image_deprecated_soon"
        column_name = ["pk", "nvarchar_text", "varchar_text", "varbinary_data",
                       "rowversion_synonym_timestamp"]
        column_type = ["int", "ntext", "text", "image", "timestamp"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name, text_values, column_name[:-1]))

        # test uniqueidentifier and rowversion
        table_name = "weirdos"
        column_name = [
            "pk", "geospacial", "geospacial_map", "markup", "guid", "tree",
            "variant", "SpecialPurposeColumns", "version"
        ]
        column_type = [
            "int", "geometry", "geography", "xml", "uniqueidentifier", "hierarchyid",
            "sql_variant", "xml COLUMN_SET FOR ALL_SPARSE_COLUMNS", "rowversion"
        ]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        # not sure why I have to do this but getting error - Parameter information is missing from a user-defined type.
        for value in other_values:
            query_list.extend(insert(database_name, schema_name, table_name, [value], column_name[:-1]))

        table_name = "computed_columns"
        column_name = ["pk", "started_at", "ended_at", "durations_days"]
        column_type = ["int", "datetimeoffset", "datetimeoffset", "AS DATEDIFF(day, started_at, ended_at)"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name, comp_values, column_name[:-1]))
        mssql_cursor_context_manager(*query_list)

        # update values with rowversions
        rows = mssql_cursor_context_manager(*["select version from data_types_database.dbo.weirdos order by pk"])
        rows = ["0x{}".format(value.hex().upper()) for value, in rows]
        cls.EXPECTED_METADATA['data_types_database_dbo_weirdos']['values'] = \
            [other_values[row] + (version,) for row, version in enumerate(rows)]

        rows = mssql_cursor_context_manager(*[
            "select rowversion_synonym_timestamp from data_types_database.dbo.text_and_image_deprecated_soon order by pk"])
        rows = ["0x{}".format(value.hex().upper()) for value, in rows]
        cls.EXPECTED_METADATA['data_types_database_dbo_text_and_image_deprecated_soon']['values'] = \
            [text_values[row] + (version,) for row, version in enumerate(rows)]

        rows = mssql_cursor_context_manager(
            *["select durations_days from data_types_database.dbo.computed_columns order by pk"])
        cls.EXPECTED_METADATA['data_types_database_dbo_computed_columns']['values'] = \
            [comp_values[row] + tuple(version) for row, version in enumerate(rows)]

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

        # Don't select unsupported data types
        non_selected_properties = ["nvarchar_text", "varchar_text", "varbinary_data",
                                   "geospacial", "geospacial_map", "markup", "tree",
                                   "variant", "SpecialPurposeColumns", "started_at", "ended_at"]
        BaseTapTest.select_all_streams_and_fields(
            conn_id, found_catalogs, additional_md=additional_md,
            non_selected_properties=non_selected_properties)

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
                            if column not in non_selected_properties
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
        table_name = "text_and_image_deprecated_soon"
        column_name = ["pk", "nvarchar_text", "varchar_text", "varbinary_data",
                       "rowversion_synonym_timestamp"]
        insert_value = [(2, "JKL", "MNO", "PQR".encode('utf-8'))]
        update_value = [(1, "JKL", "MNO", "PQR".encode('utf-8'))]
        delete_value = [(0, )]
        query_list = (insert(database_name, schema_name, table_name, insert_value, column_name[:-1]))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        values = insert_value + update_value
        rows = mssql_cursor_context_manager(*[
            "select rowversion_synonym_timestamp from data_types_database.dbo.text_and_image_deprecated_soon "
            "where pk in (0,1,2) order by pk desc"])
        rows = [tuple(row) for row in rows]
        rows = [("0x{}".format(value.hex().upper()), ) for value, in rows]
        row_with_version = [x[0] + x[1] + (None, ) for x in zip(values, rows)]
        row_with_version.append((0, None, None, None, None, datetime.utcnow()))
        row_with_version[1], row_with_version[2] = row_with_version[2], row_with_version[1]
        self.EXPECTED_METADATA['data_types_database_dbo_text_and_image_deprecated_soon']['values'] = row_with_version
        self.EXPECTED_METADATA["data_types_database_dbo_text_and_image_deprecated_soon"]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )

        database_name = "data_types_database"
        schema_name = "dbo"
        table_name = "weirdos"
        column_name = [
            "pk", "geospacial", "geospacial_map", "markup", "guid", "tree",
            "variant", "SpecialPurposeColumns", "version"]
        insert_value = [(3, None, None, None, str(uuid.uuid1()).upper(), None, None, None)]
        update_value = [(1, None, None, None, str(uuid.uuid1()).upper(), None, None, None)]
        delete_value = [(0,)]
        query_list = (insert(database_name, schema_name, table_name, insert_value, column_name[:-1]))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        values = insert_value + update_value
        rows = mssql_cursor_context_manager(*[
            "select version from data_types_database.dbo.weirdos "
            "where pk in (0,1,3) order by pk desc"])
        rows = [tuple(row) for row in rows]
        rows = [("0x{}".format(value.hex().upper()), ) for value, in rows]
        row_with_version = [x[0] + x[1] + (None, ) for x in zip(values, rows)]
        row_with_version.append((0, None, None, None, None, None, None, None, None, datetime.utcnow()))
        row_with_version[1], row_with_version[2] = row_with_version[2], row_with_version[1]
        self.EXPECTED_METADATA['data_types_database_dbo_weirdos']['values'] = row_with_version
        self.EXPECTED_METADATA["data_types_database_dbo_weirdos"]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )

        database_name = "data_types_database"
        schema_name = "dbo"
        table_name = "computed_columns"
        column_name = ["pk", "started_at", "ended_at", "durations_days"]
        insert_value = [(2, datetime(1980, 5, 30, 16), datetime.now())]
        update_value = [(1, datetime(1942, 11, 30), datetime(2017, 2, 12))]
        delete_value = [(0,)]
        query_list = (insert(database_name, schema_name, table_name, insert_value, column_name[:-1]))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        values = insert_value + update_value  # + [delete_value[0] + (None, None)]
        rows = mssql_cursor_context_manager(
            *["select durations_days from data_types_database.dbo.computed_columns "
              "where pk in (0,1,2) order by pk desc"])
        rows = [tuple(row) for row in rows]
        row_with_duration = [x[0] + x[1] + (None, ) for x in zip(values, rows)]
        row_with_duration.append((0, None, None, None, datetime.utcnow()))
        row_with_duration[1], row_with_duration[2] = row_with_duration[2], row_with_duration[1]
        self.EXPECTED_METADATA['data_types_database_dbo_computed_columns']['values'] = row_with_duration
        self.EXPECTED_METADATA["data_types_database_dbo_computed_columns"]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )

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
                            if column not in non_selected_properties
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
