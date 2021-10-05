"""
Test tap discovery
"""
from datetime import datetime, timedelta

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking, update_by_pk, delete_by_pk, \
    create_view

from base import BaseTapTest


class SyncPkLogical(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_logical_sync_pk_test".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """The expected streams and metadata about the streams"""

        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""

        drop_all_user_databases()
        database_name = "constraints_database"
        schema_name = "dbo"
        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        query_list.extend(enable_database_tracking(database_name))

        # pyodbc.ProgrammingError: ('42000', "[42000] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Cannot
        # enable change tracking on table 'no_constraints'. Change tracking requires a primary key on the table.
        # Create a primary key on the table before enabling change tracking. (4997) (SQLExecDirectW)")
        # table_name = "no_constraints"
        # cls.EXPECTED_METADATA = {
        #     '{}_{}_{}'.format(database_name, schema_name, table_name): {
        #         'is-view': False,
        #         'schema-name': schema_name,
        #         'row-count': 0,
        #         'values': [(0, ), (1, )],
        #         'table-key-properties': set(),
        #         'selected': None,
        #         'database-name': database_name,
        #         'stream_name': table_name,
        #         'fields': [
        #             {'column_name': {'sql-datatype': 'int', 'selected-by-default': True,
        #                              'inclusion': 'available'}}],
        #         'schema': {
        #             'type': 'object',
        #             'properties': {
        #                 'column_name': {
        #                     'type': ['integer', 'null'],
        #                     'minimum': -2147483648,
        #                     'maximum': 2147483647,
        #                     'inclusion': 'available',
        #                     'selected': True},
        #                 "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}},
        #             'selected': True}},
        #     }
        #
        # column_name = ["column_name"]
        # column_type = ["int"]
        # primary_key = set()
        # column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        # query_list.extend(create_table(database_name, schema_name, table_name, column_def,
        #                                primary_key=primary_key, tracking=True))
        # query_list.extend(insert(database_name, schema_name, table_name,
        #                          cls.EXPECTED_METADATA['{}_{}_{}'.format(
        #                              database_name, schema_name, table_name)]["values"]))

        table_name = "multiple_column_pk"
        primary_key = ["first_name", "last_name"]
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                ("Tim", "Berners-Lee", 64),
                ("Sergey", "Brin", 45),
                ("Larry", "Page", 46)],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                    {'first_name': {'sql-datatype': 'varchar', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'last_name': {'sql-datatype': 'varchar', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'info': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'info': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648},
                    'first_name': {
                        'type': ['string'],
                        'maxLength': 256,
                        'inclusion': 'automatic',
                        'selected': True},  # 'minLength': 0},
                    'last_name': {
                        'type': ['string'],
                        'maxLength': 256,
                        'inclusion': 'automatic',
                        'selected': True},  # 'minLength': 0},(1, 4, 2, 5)
                    "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}}}
        }
        column_name = ["first_name", "last_name", "info"]
        column_type = ["varchar(256)", "varchar(256)", "int"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True) )
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        table_name = "single_column_pk"
        primary_key = ["pk"]
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, 3),
                (1, 4),
                (2, 5)],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                {'data': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'pk': {
                        'maximum': 2147483647,
                        'type': ['integer'],
                        'inclusion': 'automatic',
                        'selected': True,
                        'minimum': -2147483648},
                    'data': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648},
                    "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}}}
        }
        column_name = ["pk", "data"]
        column_type = ["int", "int"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True) )
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        table_name = "pk_with_unique_not_null"
        primary_key = ["pk"]
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, 3),
                (1, 4),
                (2, 5)],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                {'data': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'pk': {
                        'maximum': 2147483647,
                        'type': ['integer'],
                        'inclusion': 'automatic',
                        'selected': True,
                        'minimum': -2147483648},
                    'data': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648},
                    "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}}}
        }
        column_name = ["pk", "data"]
        column_type = ["int", "int NOT NULL UNIQUE"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True) )
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        table_name = "pk_with_fk"
        primary_key = ["pk"]
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, 1),
                (1, 0),
                (2, 0),
                (3, 1),
                (4, None)],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                {'fk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'pk': {
                        'maximum': 2147483647,
                        'type': ['integer'],
                        'inclusion': 'automatic',
                        'selected': True,
                        'minimum': -2147483648},
                    'fk': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648},
                    "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}}}
        }
        column_name = ["pk", "fk"]
        column_type = ["int", "int"]
        foreign_key = "fk"
        reference = "{}.pk_with_unique_not_null(pk)".format(schema_name)
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def,
            primary_key=primary_key, foreign_key=foreign_key, reference=reference, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        # CAN'T ENABLE CHANGE TRACKING ON A VIEW
        # table_name = "view_with_join"
        # primary_key = []
        # cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
        #     'is-view': True,
        #     'schema-name': schema_name,
        #     'row-count': 0,
        #     'values': [
        #         (1, 4, 0),
        #         (0, 3, 1),
        #         (0, 3, 2),
        #         (1, 4, 3),
        #         (None, None, 4)],
        #     'table-key-properties': primary_key,
        #     'selected': None,
        #     'database-name': database_name,
        #     'stream_name': table_name,
        #     'fields': [
        #         {'column1': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}},
        #         {'data': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}},
        #         {'column2': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
        #     'schema': {
        #         'type': 'object',
        #         'selected': True,
        #         'properties': {
        #             'column1': {
        #                 'maximum': 2147483647,
        #                 'type': ['integer', 'null'],
        #                 'inclusion': 'available',
        #                 'selected': True,
        #                 'minimum': -2147483648},
        #             'data': {
        #                 'maximum': 2147483647,
        #                 'type': ['integer', 'null'],
        #                 'inclusion': 'available',
        #                 'selected': True,
        #                 'minimum': -2147483648},
        #             'column2': {
        #                 'maximum': 2147483647,
        #                 'type': ['integer'],
        #                 'inclusion': 'available',
        #                 'selected': True,
        #                 'minimum': -2147483648},
        #             "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}}}
        # }
        # select = ("SELECT p.pk as column1, data, f.pk as column2 "
        #           "FROM pk_with_unique_not_null p "
        #           "RIGHT JOIN pk_with_fk f on p.pk = f.fk")
        # query_list.extend(create_view(schema_name, table_name, select))

        # pyodbc.ProgrammingError: ('42000', "[42000] [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]Cannot
        # enable change tracking on table 'table_with_index'. Change tracking requires a primary key on the table.
        # Create a primary key on the table before enabling change tracking. (4997) (SQLExecDirectW)")
        # table_name = "table_with_index"
        # primary_key = []
        # cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
        #     'is-view': False,
        #     'schema-name': schema_name,
        #     'row-count': 0,
        #     'values': [
        #         (0, 3),
        #         (1, 4)],
        #     'table-key-properties': primary_key,
        #     'selected': None,
        #     'database-name': database_name,
        #     'stream_name': table_name,
        #     'fields': [
        #         {'not_pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}},
        #         {'data': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
        #     'schema': {
        #         'type': 'object',
        #         'selected': True,
        #         'properties': {
        #             'not_pk': {
        #                 'maximum': 2147483647,
        #                 'type': ['integer', 'null'],
        #                 'inclusion': 'available',
        #                 'selected': True,
        #                 'minimum': -2147483648},
        #             'data': {
        #                 'maximum': 2147483647,
        #                 'type': ['integer'],
        #                 'inclusion': 'available',
        #                 'selected': True,
        #                 'minimum': -2147483648},
        #             "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}}}
        # }
        # column_name = ["not_pk", "data"]
        # column_type = ["int", "int NOT NULL INDEX myindex"]
        #
        # column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        # query_list.extend(create_table(database_name, schema_name, table_name, column_def,
        #                                primary_key=primary_key, tracking=True) )
        # query_list.extend(insert(database_name, schema_name, table_name,
        #                          cls.EXPECTED_METADATA['{}_{}_{}'.format(
        #                              database_name, schema_name, table_name)]["values"]))

        table_name = "default_column"
        primary_key = ["pk"]
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, ),
                (1, )],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                {'default_column': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'pk': {
                        'maximum': 2147483647,
                        'type': ['integer'],
                        'inclusion': 'automatic',
                        'selected': True,
                        'minimum': -2147483648},
                    'default_column': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648},
                    "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}}}
        }
        column_name = ["pk", "default_column"]
        column_type = ["int", "int DEFAULT -1"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def, primary_key=primary_key, tracking=True) )
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"],
                                 column_names=["pk"]))
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)]["values"] = [
                (0, -1),
                (1, -1)]

        table_name = "check_constraint"
        primary_key = ["pk"]
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, 120),
                (1, 34)],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                {'age': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'pk': {
                        'maximum': 2147483647,
                        'type': ['integer'],
                        'inclusion': 'automatic',
                        'selected': True,
                        'minimum': -2147483648},
                    'age': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648},
                    "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}}}
        }
        column_name = ["pk", "age"]
        column_type = ["int", "int CHECK (age <= 120)"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def, primary_key=primary_key, tracking=True) )
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        table_name = "even_identity"
        primary_key = ["pk"]
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (1, ),
                (2, )],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                {'even_id': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'pk': {
                        'maximum': 2147483647,
                        'type': ['integer'],
                        'inclusion': 'automatic',
                        'selected': True,
                        'minimum': -2147483648},
                    'even_id': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648},
                    "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}}}
        }
        column_name = ["pk", "even_id"]
        column_type = ["int", "int IDENTITY(2,2)"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def, primary_key=primary_key, tracking=True) )
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"],
                                 column_names=["pk"]))
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)]["values"] = [
            (1, 2),
            (2, 4)]
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

        database_name = "constraints_database"
        schema_name = "dbo"
        table_name = "multiple_column_pk"
        column_name = ["first_name", "last_name", "info"]
        insert_value = [("Brian", "Lampkin", 49)]
        update_value = [("Tim", "Berners-Lee", 65)]
        delete_value = [("Larry", "Page")]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:2]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = [insert_value[0] + (None,)]
        update_value = [update_value[0] + (None,)]
        delete_value = [delete_value[0] + (None, datetime.utcnow())]
        self.EXPECTED_METADATA["constraints_database_dbo_multiple_column_pk"]["values"] = \
            insert_value + delete_value + update_value
        self.EXPECTED_METADATA["constraints_database_dbo_multiple_column_pk"]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )

        table_name = "single_column_pk"
        column_name = ["pk", "data"]
        insert_value = [(3, 49)]
        update_value = [(1, 65)]
        delete_value = [(0, )]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = [insert_value[0] + (None,)]
        update_value = [update_value[0] + (None,)]
        delete_value = [delete_value[0] + (None, datetime.utcnow())]
        self.EXPECTED_METADATA["constraints_database_dbo_single_column_pk"]["values"] = \
            insert_value + delete_value + update_value
        self.EXPECTED_METADATA["constraints_database_dbo_single_column_pk"]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )

        table_name = "pk_with_fk"
        column_name = ["pk", "fk"]
        insert_value = [(5, 2)]
        update_value = [(0, 2)]
        delete_value = [(1, ), (2, )]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = [insert_value[0] + (None,)]
        update_value = [update_value[0] + (None,)]
        delete_value = [delete_value[0] + (None, datetime.utcnow()),
                        delete_value[1] + (None, datetime.utcnow())]
        self.EXPECTED_METADATA["constraints_database_dbo_pk_with_fk"]["values"] = \
            insert_value + delete_value + update_value
        self.EXPECTED_METADATA["constraints_database_dbo_pk_with_fk"]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )

        # TODO BUG - https://stitchdata.atlassian.net/browse/SRCE-1300
        table_name = "pk_with_unique_not_null"
        column_name = ["pk", "data"]
        insert_value = [(3, 49)]
        update_value = [(1, 65)]
        delete_value = [(0, )]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = [insert_value[0] + (None,)]
        update_value = [update_value[0] + (None,)]
        delete_value = [delete_value[0] + (None, datetime.utcnow())]
        self.EXPECTED_METADATA["constraints_database_dbo_pk_with_unique_not_null"]["values"] = \
            insert_value + delete_value + update_value  # TODO - add back delete_value
        self.EXPECTED_METADATA["constraints_database_dbo_pk_with_unique_not_null"]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )

        table_name = "default_column"
        column_name = ["pk", "default_column"]
        insert_value = [(3, 49)]
        update_value = [(1, 65)]
        delete_value = [(0,)]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = [insert_value[0] + (None,)]
        update_value = [update_value[0] + (None,)]
        delete_value = [delete_value[0] + (None, datetime.utcnow())]
        self.EXPECTED_METADATA["constraints_database_dbo_default_column"]["values"] = \
            insert_value + delete_value + update_value
        self.EXPECTED_METADATA["constraints_database_dbo_default_column"]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )

        table_name = "check_constraint"
        column_name = ["pk", "age"]
        insert_value = [(3, 49)]
        update_value = [(1, 65)]
        delete_value = [(0,)]
        query_list = (insert(database_name, schema_name, table_name, insert_value))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = [insert_value[0] + (None,)]
        update_value = [update_value[0] + (None,)]
        delete_value = [delete_value[0] + (None, datetime.utcnow())]
        self.EXPECTED_METADATA["constraints_database_dbo_check_constraint"]["values"] = \
            insert_value + delete_value + update_value
        self.EXPECTED_METADATA["constraints_database_dbo_check_constraint"]["fields"].append(
            {"_sdc_deleted_at": {
                'sql-datatype': 'datetime', 'selected-by-default': True, 'inclusion': 'automatic'}}
        )

        # TODO BUG - https://stitchdata.atlassian.net/browse/SRCE-1300
        table_name = "even_identity"
        column_name = ["pk", "even_id"]
        insert_value = [(3,)]
        update_value = [(1, )]
        delete_value = [(2,)]
        query_list = (insert(database_name, schema_name, table_name, insert_value, column_names=column_name[:1]))
        query_list.extend(delete_by_pk(database_name, schema_name, table_name, delete_value, column_name[:1]))
        query_list.extend(update_by_pk(database_name, schema_name, table_name, update_value, column_name))
        mssql_cursor_context_manager(*query_list)
        insert_value = [insert_value[0] + (6, None)]
        update_value = [update_value[0] + (2, None)]
        delete_value = [delete_value[0] + (None, datetime.utcnow())]
        self.EXPECTED_METADATA["constraints_database_dbo_even_identity"]["values"] = \
             insert_value + delete_value + update_value  # TODO - BUG add back delete_value +
        self.EXPECTED_METADATA["constraints_database_dbo_even_identity"]["fields"].append(
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
