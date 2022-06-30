import unittest

from tap_tester import connections, menagerie, runner

from base import BaseTapTest

from database import get_test_connection, drop_all_user_databases, create_database, create_table, mssql_cursor_context_manager, insert


class ChangeReplicationTest(BaseTapTest):

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_change_replication_test".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """ Expected streams and metadata about the streams """
        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""
        drop_all_user_databases()
        global database_name
        database_name = "change_replication"
        global schema_name
        schema_name = "dbo"

        int_values = [(0, 0, False),(1, 255, True),(2, 42, None),(3, 230, False),
                      (4, 6, True),(5, 236, True),(6, 27, True),(7, 132, True),
                      (8, 251, False),(9, 187, True),(10, 157, True),(11, 51, True),(12, 144, True)]

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
            'int_data': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': int_values,
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

        table_name = "int_data"
        column_name = ["pk", "MyTinyIntColumn", "my_boolean"]
        column_type = ["int", "tinyint", "bit"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name, int_values))

        cls.expected_metadata = cls.discovery_expected_metadata

        mssql_cursor_context_manager(*query_list)

    def expected_sync_streams(self):
        return {'change_replication_dbo_int_data'}

    def expected_primary_keys_by_sync_stream_id(self):
        return {'change_replication_dbo_int_data': {'pk'}}

    def expected_count(self):
        return {'change_replication_dbo_int_data': 13}

    def expected_count_1(self):
        return {'change_replication_dbo_int_data': 15}

    def expected_count_2(self):
        return {'change_replication_dbo_int_data': 2}

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
        additional_md = [{"breadcrumb": [], "metadata": {'replication-method': 'FULL_TABLE'}}]
        BaseTapTest.select_all_streams_and_fields(
            conn_id, found_catalogs, additional_md=additional_md, non_selected_properties=[])

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify state and bookmark
        initial_state = menagerie.get_state(conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        records_by_stream_1 = runner.get_records_from_target_output()
        record_count_by_stream_1 = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_primary_keys_by_sync_stream_id())

        first_bookmark = initial_state['bookmarks']
        first_bookmark_version = first_bookmark['change_replication_dbo_int_data']['version']

        self.assertEqual(first_bookmark_version, records_by_stream_1['change_replication_dbo_int_data']['table_version'])
        self.assertTrue('replication_key' not in first_bookmark)
        self.assertEqual(record_count_by_stream_1, self.expected_count())

        ############ insert few records and then change replication method to log based and sync again ##############

        query_list = []
        table_name = 'int_data'
        additional_int_values = [(13, 55, True), (14, 44, True)]
        query_list.extend(insert(database_name, schema_name, table_name, additional_int_values))

        mssql_cursor_context_manager(*query_list)

        additional_md = [{"breadcrumb": [], "metadata": {'replication-method': 'INCREMENTAL',
                                                         'replication_key': 'pk'}}]
        BaseTapTest.select_all_streams_and_fields(
            conn_id, found_catalogs, additional_md=additional_md, non_selected_properties=[])

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify state and bookmark
        second_state = menagerie.get_state(conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        records_by_stream_2  = runner.get_records_from_target_output()
        record_count_by_stream_2 = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_primary_keys_by_sync_stream_id())

        second_bookmark = second_state['bookmarks']
        second_bookmark_version = second_bookmark['change_replication_dbo_int_data']['version']

        # BUG : TDL-19687 : Missing 'last_replication_method' in bookmark

        self.assertEqual(second_bookmark_version, records_by_stream_2['change_replication_dbo_int_data']['table_version'])
        self.assertTrue('replication_key_value' in second_bookmark['change_replication_dbo_int_data'])
        self.assertTrue('replication_key_name' in second_bookmark['change_replication_dbo_int_data'])
        self.assertEqual(14, second_bookmark['change_replication_dbo_int_data']['replication_key_value'])
        self.assertEqual(record_count_by_stream_2, self.expected_count_1())

        # validate the version has incremented
        # BUG : TDL-19690 : Table version is not getting incremented when replication method is changed
        #self.assertGreater(second_bookmark_version, first_bookmark_version)

        # Add another record and run sync again
        query_list = []
        table_name = 'int_data'
        additional_int_values = [(15, 99, True)]
        query_list.extend(insert(database_name, schema_name, table_name, additional_int_values))

        mssql_cursor_context_manager(*query_list)

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify state and bookmark
        final_state = menagerie.get_state(conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        records_by_stream_3  = runner.get_records_from_target_output()
        record_count_by_stream_3 = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_primary_keys_by_sync_stream_id())

        third_bookmark = final_state['bookmarks']
        third_bookmark_version = second_bookmark['change_replication_dbo_int_data']['version']

        # BUG : TDL-19687 : Missing 'last_replication_method' in bookmark

        self.assertEqual(third_bookmark_version, records_by_stream_3['change_replication_dbo_int_data']['table_version'])
        self.assertTrue('replication_key_value' in third_bookmark['change_replication_dbo_int_data'])
        self.assertTrue('replication_key_name' in third_bookmark['change_replication_dbo_int_data'])
        self.assertEqual(15, third_bookmark['change_replication_dbo_int_data']['replication_key_value'])
        self.assertEqual(record_count_by_stream_3, self.expected_count_2())
