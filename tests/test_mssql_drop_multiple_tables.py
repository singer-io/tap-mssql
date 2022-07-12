import unittest

from tap_tester import connections, menagerie, runner

from base import BaseTapTest

from database import drop_all_user_databases, create_database, create_table, mssql_cursor_context_manager, insert, drop_table


class MssqlDropTables(BaseTapTest):
    """
    The objective of this test is to ensure we maintain table/field selection in
    metadata to prevent losing connection to the source database, in the case where
    all tables in a database are suddenly dropped.
    This situation occurs for some clients with scheduled database processes
    where their tables are frequently dropped and recreated.
    """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_drop_multiple_table_test".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """ Expected streams and metadata about the streams """
        return cls.EXPECTED_METADATA

    @staticmethod
    def expected_check_streams():
        return {'int_data_1', 'int_data_2'}

    def expected_sync_streams(self):
        return {'drop_multiple_tables_dbo_int_data_1', 'drop_multiple_tables_dbo_int_data_2'}

    def expected_primary_keys_by_sync_stream_id(self):
        return {'drop_multiple_tables_dbo_int_data_1': {'pk'}, 'drop_multiple_tables_dbo_int_data_2': {'pk'}}

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""
        drop_all_user_databases()
        global database_name
        database_name = "drop_multiple_tables"
        global schema_name
        schema_name = "dbo"

        streams = cls.expected_check_streams()
        for stream in streams:
            query_list = []
            query_list.extend(drop_table(database_name, schema_name, stream))
            mssql_cursor_context_manager(*query_list)

        int_values_1 = [(0, 0, False), (1, 255, True), (2, 42, None), (3, 230, False),
                        (4, 6, True), (5, 236, True), (6, 27, True), (7, 132, True)]

        int_values_2 = [(8, 251, False), (9, 187, True), (10, 157, True), (11, 51, True), (12, 144, True)]

        int_schema = {
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
                            'selected': True}},
                    'selected': True}

        cls.EXPECTED_METADATA = {
            'int_data_1': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': int_values_1,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': 'int_data',
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'MyTinyIntColumn': {'sql-datatype': 'tinyint', 'selected-by-default': True,
                                         'inclusion': 'available'}},
                    {'my_boolean': {'sql-datatype': 'bit', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': int_schema},
            'int_data_2': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': int_values_2,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': 'int_data',
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'MyTinyIntColumn': {'sql-datatype': 'tinyint', 'selected-by-default': True,
                                         'inclusion': 'available'}},
                    {'my_boolean': {'sql-datatype': 'bit', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': int_schema}
            }

        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))

        table_name = "int_data_1"
        column_name = ["pk", "MyTinyIntColumn", "my_boolean"]
        column_type = ["int", "tinyint", "bit"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name, int_values_1))

        table_name = "int_data_2"
        column_name = ["pk", "MyTinyIntColumn", "my_boolean"]
        column_type = ["int", "tinyint", "bit"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name, int_values_2))

        cls.expected_metadata = cls.discovery_expected_metadata

        mssql_cursor_context_manager(*query_list)

    def test_run(self):

        print("running test {}".format(self.name()))

        conn_id = self.create_connection()

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # get the catalog information of discovery
        found_catalogs = menagerie.get_catalogs(conn_id)
        first_sync_catalog = [catalog for catalog in menagerie.get_catalogs(conn_id)
                              if catalog['tap_stream_id'] in self.expected_sync_streams()]
        additional_md = [{"breadcrumb": [], "metadata": {'replication-method': 'FULL_TABLE'}}]
        BaseTapTest.select_all_streams_and_fields(
            conn_id, found_catalogs, additional_md=additional_md, non_selected_properties=[])

        for stream in ['int_data_1', 'int_data_2']:
            query_list = []
            query_list.extend(drop_table(database_name, schema_name, stream))
            mssql_cursor_context_manager(*query_list)

        # run in check mode again, there should not be any tables in the database
        #check_job_name = runner.run_check_mode(self, conn_id)
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)

        # Assert that expected tables are still selected
        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):

                test_catalog = [catalog for catalog in first_sync_catalog if catalog['tap_stream_id'] == stream][0]
                md = menagerie.get_annotated_schema(conn_id, test_catalog['stream_id'])['metadata']
                self.assertTrue(all([entry['metadata']['selected'] for entry in md]))
