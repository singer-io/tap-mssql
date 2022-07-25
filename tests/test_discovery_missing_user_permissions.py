"""
Test tap discovery
"""
import os

from tap_tester import menagerie, runner

from database import mssql_cursor_context_manager, \
    drop_all_user_databases, create_database, use_db, \
create_table, insert, enable_database_tracking

from base import BaseTapTest


USERNAME = os.getenv("STITCH_TAP_MSSQL_TEST_DATABASE_USER")
TEST_USERNAME = "sloth"
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
        return "{}_disco_perms".format(super().name())

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
        mssql_cursor_context_manager(*query_list)

    def scenario_setup_steps(self):
        """
        Execute the following steps:
          Create a connection with TEST_USERNAME.
          Run the initial check job.
          Select logical replication.
          Run the historical sync job.

        :return: historical sync job id and exit status
        """
        # create connection with the new user and run initial check job
        os.environ["STITCH_TAP_MSSQL_TEST_DATABASE_USER_2"] = TEST_USERNAME
        self.CONFIGURATION_ENVIRONMENT['properties']['user'] = "STITCH_TAP_MSSQL_TEST_DATABASE_USER_2"
        conn_id = self.create_connection()

        # Verify number of actual streams discovered match expected
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0)
        self.assertEqual(len(found_catalogs), len(self.expected_streams()))

        # Verify the stream names discovered were what we expect
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertEqual(set(self.expected_streams()), set(found_catalog_names))

        # get the catalog information of discovery
        additional_md = [{"breadcrumb": [], "metadata": {'replication-method': 'LOG_BASED'}}]
        BaseTapTest.select_all_streams_and_fields(
            conn_id, found_catalogs, additional_md=additional_md, non_selected_properties=[])

        # Run a sync and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)

        return conn_id, sync_job_name, exit_status

    def test_user_missing_change_tracking(self):
        """
        Verify the tap throws an error message for logical replication if
        the user does not have VIEW CHANGE TRACKING permission on the stream.
        """

        print("running test {}".format(self.name()))

        # Create the user and login 
        query_list = use_db(database_name)
        query_list.extend([f"CREATE LOGIN {TEST_USERNAME} WITH PASSWORD='{PASSWORD}';"])
        query_list.extend([f'CREATE USER {TEST_USERNAME} FOR LOGIN {TEST_USERNAME};'])

        # Explicitly give user permission prior to connection check and initial sync
        query_list.extend([f'GRANT SELECT ON "{schema_name}"."{table_name}" TO {TEST_USERNAME};'])
        # query_list.extend([f'GRANT VIEW CHANGE TRACKING ON "{schema_name}"."{table_name}" TO {TEST_USERNAME};'])
        results = mssql_cursor_context_manager(*query_list)

        # Create a connection with the test user,
        # set it for logical replication, and execute the historical sync.
        conn_id, sync_job_name, exit_status = self.scenario_setup_steps()
        error_message = exit_status['tap_error_message']

        # Verify the sync fails and explicitly calls the missing permission
        self.assertEqual(exit_status['tap_exit_status'], 1)
        self.assertIn("The VIEW CHANGE TRACKING permission was denied", error_message)

        # Verfiy the database, schema and table are specified in the 
        self.assertIn(table_name, error_message)
        self.assertIn(database_name, error_message)
        self.assertIn(schema_name, error_message)

    def test_user_missing_select(self):
        """
        Verify the tap throws an error message for logical replication if
        the user does not have SELECT permission on the stream.
        """

        print("running test {}".format(self.name()))

        # Create the user and login 
        query_list = use_db(database_name)
        query_list.extend([f"CREATE LOGIN {TEST_USERNAME} WITH PASSWORD='{PASSWORD}';"])
        query_list.extend([f'CREATE USER {TEST_USERNAME} FOR LOGIN {TEST_USERNAME};'])

        # Explicitly give user permission prior to connection check and initial sync
        # query_list.extend([f'GRANT SELECT ON "{schema_name}"."{table_name}" TO {TEST_USERNAME};'])
        query_list.extend([f'GRANT VIEW CHANGE TRACKING ON "{schema_name}"."{table_name}" TO {TEST_USERNAME};'])
        results = mssql_cursor_context_manager(*query_list)

        # Create a connection with the test user,
        # set it for logical replication, and execute the historical sync.
        conn_id, sync_job_name, exit_status = self.scenario_setup_steps()
        error_message = exit_status['tap_error_message']

        # Verify the sync fails and explicitly calls the missing permission
        self.assertEqual(exit_status['tap_exit_status'], 1)
        self.assertIn("The SELECT permission was denied", error_message)

        # Verfiy the database, schema and table are specified in the 
        self.assertIn(table_name, error_message)
        self.assertIn(database_name, error_message)
        self.assertIn(schema_name, error_message)

    def test_user_removed_logical(self):
        """
        Verify the tap throws an error message for logical replication if
        a valid user with permissions to the stream is removed from the server between logical syncs.
        """
        print("running test {}".format(self.name()))

        # Create the user and login 
        query_list = use_db(database_name)
        query_list.extend([f"CREATE LOGIN {TEST_USERNAME} WITH PASSWORD='{PASSWORD}';"])
        query_list.extend([f'CREATE USER {TEST_USERNAME} FOR LOGIN {TEST_USERNAME};'])

        # Explicitly give user permission prior to connection check and initial sync
        query_list.extend([f'GRANT SELECT ON "{schema_name}"."{table_name}" TO {TEST_USERNAME};'])
        query_list.extend([f'GRANT VIEW CHANGE TRACKING ON "{schema_name}"."{table_name}" TO {TEST_USERNAME};'])
        results = mssql_cursor_context_manager(*query_list)

        # Create a connection with the test user,
        # set it for logical replication, and execute the historical sync.
        conn_id, sync_job_name, exit_status = self.scenario_setup_steps()
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # insert some data to ensure the change tracking has new data to return
        query_list = []
        int_after_values = [(6, 22, 44),
                            (7, 0, 0),
                            (8, 23, 43),
                            (9, None, None),
                            (10, 24, 44),
                            (11, 25, 45)]
        query_list.extend(insert(database_name, schema_name, table_name,
                                 int_after_values))
        
        # remove the user with read permissions
        query_list = use_db(database_name)
        query_list.append(f"DROP USER {TEST_USERNAME}")
        query_list.append(f"DROP LOGIN {TEST_USERNAME}")
        mssql_cursor_context_manager(*query_list)

        # Run a sync and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)

        # validate the exit status message on table which does not have proper user permissions
        self.assertEqual(exit_status['discovery_exit_status'], 1)
        self.assertIn(f"Login failed for user '{TEST_USERNAME}'", exit_status['discovery_error_message'])
