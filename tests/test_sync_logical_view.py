"""
Test tap logical replication for views
"""
from datetime import datetime, timedelta

from tap_tester import menagerie, runner, LOGGER

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking, update_by_pk, delete_by_pk, \
    create_view

from base import BaseTapTest


class SyncViewLogical(BaseTapTest):
    """ Test the tap logical replication for views """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_logical_sync_view_test".format(super().name())

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
                        'type': ['integer'],
                        'inclusion': 'available',
                        'selected': True,
                        'minimum': -2147483648},
                    "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}}}
        }
        select = ("SELECT p.pk as column1, data, f.pk as column2 "
                  "FROM pk_with_unique_not_null p "
                  "RIGHT JOIN pk_with_fk f on p.pk = f.fk")
        query_list.extend(create_view(schema_name, table_name, select))

        mssql_cursor_context_manager(*query_list)

        cls.expected_metadata = cls.discovery_expected_metadata

    def test_run(self):
        """
        Verify that attempting to do logical replication on a view generates the expected
        tap exit status and error message
        """

        LOGGER.info("running test %s", self.name())

        conn_id = self.create_connection()

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # get the catalog information of discovery
        found_catalogs = menagerie.get_catalogs(conn_id)
        additional_md = [{"breadcrumb": [], "metadata": {'replication-method': 'LOG_BASED'}}]
        self.select_all_streams_and_fields(conn_id, found_catalogs, additional_md=additional_md)

        # run a sync and verify exit codes
        menagerie.set_state(conn_id, {})
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        name_of_view = self.expected_metadata()['constraints_database_dbo_view_with_join']['stream_name']

        self.assertEqual(1, exit_status['tap_exit_status'])
        self.assertIn('Cannot sync stream', exit_status['tap_error_message'])
        self.assertIn('using log-based replication', exit_status['tap_error_message'])
        self.assertIn('Change Tracking is not enabled for table', exit_status['tap_error_message'])
        self.assertIn(name_of_view, exit_status['tap_error_message'])
