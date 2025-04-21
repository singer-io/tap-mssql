import unittest

from tap_tester import connections, menagerie, runner, LOGGER

from base import BaseTapTest

from database import drop_all_user_databases, create_database, create_table, mssql_cursor_context_manager, insert, enable_database_tracking


class LogBasedNoPkTest(BaseTapTest):

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_log_based_no_pk_test".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """ Expected streams and metadata about the streams """
        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""
        drop_all_user_databases()
        global database_name
        database_name = "log_based_no_pk"
        global schema_name
        schema_name = "dbo"

        int_values = [(0, 0, False), (1, 255, True), (2, 42, None), (3, 230, False)]
        int_values_no_pk = [(1, 1, True), (1, 1, True), (1, 2, False)]

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

        global query_list
        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        query_list.extend(enable_database_tracking(database_name))

        table_name = "int_data"
        column_name = ["pk", "MyTinyIntColumn", "my_boolean"]
        column_type = ["int", "tinyint", "bit"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name, int_values))

        table_name = "int_data_no_pk"
        column_name = ["pk", "MyTinyIntColumn", "my_boolean"]
        column_type = ["int", "tinyint", "bit"]
        primary_key = {}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name, int_values_no_pk))

        mssql_cursor_context_manager(*query_list)

        cls.expected_metadata = cls.discovery_expected_metadata

    def test_run(self):

        LOGGER.info("running test %s", self.name())

        """
        MSSQL does not allow change tracking to be enabled for a table
        with no primary key, so we need to assert if this raises an exception
        """
        conn_id = self.create_connection()

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # get the catalog information of discovery
        found_catalogs = menagerie.get_catalogs(conn_id)
        additional_md = [{"breadcrumb": [], "metadata": {'replication-method': 'LOG_BASED'}}]
        BaseTapTest.select_all_streams_and_fields(
            conn_id, found_catalogs, additional_md=additional_md, non_selected_properties=[])

        # Run a sync and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)

        # validate the exit status message on table which does not have pk
        self.assertEqual(exit_status['tap_error_message'], '[main] tap-mssql.core - Fatal Error Occured - Cannot sync stream: log_based_no_pk_dbo_int_data_no_pk using log-based replication. Change Tracking is not enabled for table: int_data_no_pk')
