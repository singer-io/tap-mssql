"""
Test tap discovery
"""

from decimal import Decimal

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, create_view

from base import BaseTapTest


class SyncIntFull(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_full_sync_pk_test".format(super().name())

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
        # query_list.extend(create_schema(database_name, schema_name))

        table_name = "no_constraints"
        cls.EXPECTED_METADATA = {
            '{}_{}_{}'.format(database_name, schema_name, table_name): {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': [(0, ), (1, )],
                'table-key-properties': set(),
                'selected': None,
                'database-name': database_name,
                'stream_name': table_name,
                'fields': [
                    {'column_name': {'sql-datatype': 'int', 'selected-by-default': True,
                                     'inclusion': 'available'}}],
                'schema': {
                    'type': 'object',
                    'properties': {
                        'column_name': {
                            'type': ['integer', 'null'],
                            'minimum': -2147483648,
                            'maximum': 2147483647,
                            'inclusion': 'available',
                            'selected': True}},
                    'selected': True}},
            }

        column_name = ["column_name"]
        column_type = ["int"]
        primary_key = set()
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

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
                        'selected': True},  # , 'minLength': 0},
                    'last_name': {
                        'type': ['string'],
                        'maxLength': 256,
                        'inclusion': 'automatic',
                        'selected': True}}}  # 'minLength': 0}}}
        }
        column_name = ["first_name", "last_name", "info"]
        column_type = ["varchar(256)", "varchar(256)", "int"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
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
                (1, 4)],
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
                        'minimum': -2147483648}}}
        }
        column_name = ["pk", "data"]
        column_type = ["int", "int"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
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
                (1, 4)],
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
                        'minimum': -2147483648}}}
        }
        column_name = ["pk", "data"]
        column_type = ["int", "int NOT NULL UNIQUE"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
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
                        'minimum': -2147483648}}}
        }
        column_name = ["pk", "fk"]
        column_type = ["int", "int"]
        foreign_key = "fk"
        reference = "{}.pk_with_unique_not_null(pk)".format(schema_name)
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def,
            primary_key=primary_key, foreign_key=foreign_key, reference=reference))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

        table_name = "view_with_join"
        primary_key = []
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': True,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (1, 4, 0),
                (0, 3, 1),
                (0, 3, 2),
                (1, 4, 3),
                (None, None, 4)],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                {'column1': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}},
                {'data': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}},
                {'column2': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'column1': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648},
                    'data': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648},
                    'column2': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648}}}
        }
        select = ("SELECT p.pk as column1, data, f.pk as column2 "
                  "FROM pk_with_unique_not_null p "
                  "RIGHT JOIN pk_with_fk f on p.pk = f.fk")
        query_list.extend(create_view(schema_name, table_name, select))

        table_name = "table_with_index"
        primary_key = []
        cls.EXPECTED_METADATA['{}_{}_{}'.format(database_name, schema_name, table_name)] = {
            'is-view': False,
            'schema-name': schema_name,
            'row-count': 0,
            'values': [
                (0, 3),
                (1, 4)],
            'table-key-properties': primary_key,
            'selected': None,
            'database-name': database_name,
            'stream_name': table_name,
            'fields': [
                {'not_pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}},
                {'data': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}}],
            'schema': {
                'type': 'object',
                'selected': True,
                'properties': {
                    'not_pk': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648},
                    'data': {
                        'maximum': 2147483647,
                        'type': ['integer', 'null'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648}}}
        }
        column_name = ["not_pk", "data"]
        column_type = ["int", "int NOT NULL INDEX myindex"]

        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA['{}_{}_{}'.format(
                                     database_name, schema_name, table_name)]["values"]))

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
                        'minimum': -2147483648}}}
        }
        column_name = ["pk", "default_column"]
        column_type = ["int", "int DEFAULT -1"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def, primary_key=primary_key))
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
                        'minimum': -2147483648}}}
        }
        column_name = ["pk", "age"]
        column_type = ["int", "int CHECK (age <= 120)"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def, primary_key=primary_key))
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
                        'minimum': -2147483648}}}
        }
        column_name = ["pk", "even_id"]
        column_type = ["int", "int IDENTITY(2,2)"]
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(
            database_name, schema_name, table_name, column_def, primary_key=primary_key))
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

                expected_schemas = self.expected_metadata()[stream]['schema']
                self.assertEqual(records_by_stream[stream]['schema'],
                                 expected_schemas,
                                 msg="expected: {} != actual: {}".format(expected_schemas,
                                                                         records_by_stream[stream]['schema']))
