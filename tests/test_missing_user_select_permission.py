"""
Test tap discovery
"""
import os

from tap_tester import menagerie, runner, connections

from database import mssql_cursor_context_manager, \
    drop_all_user_databases, create_database, use_db, \
create_table, insert, enable_database_tracking

from base import BaseTapTest


TEST_USERNAME = os.getenv("TAP_MSSQL_USERNAME_SECONDARY")
PASSWORD = os.getenv("STITCH_TAP_MSSQL_TEST_DATABASE_PASSWORD")

database_name = "test_user_perms"
schema_name = "dbo"
table_name = "integers"

class DiscoveryTestUserPermissions(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    @staticmethod
    def expected_streams():
        return {f'{database_name}_{schema_name}_{table_name}'}

    def name(self):
        return "{}_bad_select_perms".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """The expected streams and metadata about the streams"""

        return cls.EXPECTED_METADATA

    def tearDown(self):
        """Drop the user if it still exists."""
        query_list = use_db(database_name)
        query_list.extend([f"SELECT name FROM sys.server_principals;"])
        results = mssql_cursor_context_manager(*query_list)
        usernames = [names[0] for names in results]
        if TEST_USERNAME in usernames:
            query_list = use_db(database_name)
            query_list.append(f"DROP USER {TEST_USERNAME}")
            query_list.append(f"DROP LOGIN {TEST_USERNAME}")
            mssql_cursor_context_manager(*query_list)
        self.CONFIGURATION_ENVIRONMENT['properties']['user'] = "STITCH_TAP_MSSQL_TEST_DATABASE_USER"

    def setUp(self) -> None:
        """Create the test database and table. Insert some data to the source."""
        # create database
        drop_all_user_databases()
        query_list = list(create_database(database_name,
                                          "Latin1_General_CS_AS"))
        query_list.extend(enable_database_tracking(database_name))

        # create table and insert data
        column_name = ["pk", "MyIntColumn", "MySmallIntColumn"]
        column_type = ["int", "int", "smallint"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        int_values = [(0, 22, 44),
                      (1, 0, 0),
                      (2, 23, 43),
                      (3, None, None),
                      (4, 24, 44),
                      (5, 25, 45)]

        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name, int_values))

        # execute queries
        mssql_cursor_context_manager(*query_list)

        # Create the user and login
        query_list = use_db(database_name)
        query_list.extend([f"CREATE LOGIN {TEST_USERNAME} WITH PASSWORD='{PASSWORD}';"])
        query_list.extend([f'CREATE USER {TEST_USERNAME} FOR LOGIN {TEST_USERNAME};'])

        # set the new user as the 'user' configurable property
        self.CONFIGURATION_ENVIRONMENT['properties']['user'] = "TAP_MSSQL_USERNAME_SECONDARY"

        # execute queries
        mssql_cursor_context_manager(*query_list)

    def _test_user_missing_select_standard(self):
        """
        Verify the tap throws an error message if the user does not have SELECT
        permission on the table for each replication method
        """
        print(f"running test {self.name()}")

        # skip granting SELECT on tables
        # create connection with the new user
        conn_id = connections.ensure_connection(self)

        #  run initial check job using orchestrator (discovery)
        check_job_name = runner.run_check_mode(self, conn_id)

        # Assert that the check job failed to discover any streams (since SELECT is not available)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        error_message = exit_status['discovery_error_message']

        # Verify number of actual streams discovered match expected
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertEqual(len(found_catalogs), 0)

        # Verify the sync fails and explicitly calls the missing permission
        self.assertEqual(exit_status['discovery_exit_status'], 1)
        # self.assertIn("The SELECT permission was denied", error_message) # MISSING BUG?

        # # Verfiy the database, schema and table are specified in the
        # self.assertIn(table_name, error_message)
        # self.assertIn(database_name, error_message)
        # self.assertIn(schema_name, error_message)

        self.assertIn("Empty Catalog: did not discover any streams", error_message)

    def test_user_missing_select_has_view_change_tracking(self):
        """
        Verify the tap throws an error message during sync if the user does not have SELECT
        permission on the table but does have VIEW CHANGE TRACKING.

        NB: This test could be extended to Full Table and Incremental methods, however it seems
            unlikely for a user to be granted VIEW CHANGE TRACKING if the connection is not going
            to be set for Log Based replication. The case where both permissions are not applied would
            fail at the initial check job and is covered in a separate test.

        """
        replication_method = 'LOG_BASED'

        print(f"running test {self.name()} against {replication_method}")

        # skip granting SELECT on tables
        # but allow VIEW CHANGE TRACKING
        query_list = use_db(database_name)
        query_list.extend([f'GRANT VIEW CHANGE TRACKING ON "{schema_name}"."{table_name}" TO {TEST_USERNAME};'])
        results = mssql_cursor_context_manager(*query_list)

        # create connection with the new user and run initial check job
        conn_id = self.create_connection()

        # Verify number of actual streams discovered match expected
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0)
        self.assertEqual(len(found_catalogs), len(self.expected_streams()))

        # Verify the stream names discovered were what we expect
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertEqual(set(self.expected_streams()), set(found_catalog_names))

        # get the catalog information of discovery
        additional_md = [{"breadcrumb": [], "metadata": {'replication-method': replication_method}}]
        BaseTapTest.select_all_streams_and_fields(
            conn_id, found_catalogs, additional_md=additional_md, non_selected_properties=[])

        # Run a sync and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)

        error_message = exit_status['tap_error_message']

        # Verify the sync fails and explicitly calls the missing permission
        self.assertEqual(exit_status['tap_exit_status'], 1)
        self.assertIn("The SELECT permission was denied", error_message)

        # Verfiy the database, schema and table are specified in the
        self.assertIn(table_name, error_message)
        self.assertIn(database_name, error_message)
        self.assertIn(schema_name, error_message)
