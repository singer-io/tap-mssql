"""
Test tap discovery
"""
from copy import deepcopy
from datetime import datetime, timedelta

from tap_tester import menagerie, runner
from tap_tester.logger import LOGGER

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking

from base import BaseTapTest


database_name = "row_version_state"
schema_name = "dbo"
table_name_1 = "row_version_1"  # sync completed
table_name_2 = f"row_version_2"  # sync interrupted
table_name_3 = f"row_version_3"  # sync not started

class SyncLogicalRowVersion(BaseTapTest):
    """ Test the tap interrupted state with rowversion """

    streams_to_test = {
        f"{database_name}_{schema_name}_{table_name_1}",
        f"{database_name}_{schema_name}_{table_name_2}",
        f"{database_name}_{schema_name}_{table_name_3}",
    }

    def name(self):
        return "{}_log_rowversion".format(super().name())

    @classmethod
    def setUpClass(cls) -> None:
        """Create the test database and tables"""

        drop_all_user_databases()

        # create the database with change tracking enabled
        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        query_list.extend(enable_database_tracking(database_name))


        # create the 'sync completed' table
        column_names = ["pk_col", "col", "rk_col"]
        column_type = ["int", "char", "rowversion"]
        primary_key = {"pk_col"}
        column_def = [" ".join(x) for x in list(zip(column_names, column_type))]
        row_values = [(i, 'a') for i in range(1000)]

        query_list.extend(create_table(database_name, schema_name, table_name_1, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(
            insert(database_name, schema_name, table_name_1, row_values, column_names=column_names[:-1]
            )
        )

        # create the 'sync interrupted' table
        query_list.extend(create_table(database_name, schema_name, table_name_2, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(
            insert(database_name, schema_name, table_name_2, row_values, column_names=column_names[:-1]
            )
        )

        # create the 'not yet synced' table
        query_list.extend(create_table(database_name, schema_name, table_name_3, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(
            insert(database_name, schema_name, table_name_3, row_values, column_names=column_names[:-1]
            )
        )

        mssql_cursor_context_manager(*query_list)

    def test_run(self):
        """
        Verify that a full sync can send capture all data and send it in the correct format
        for integer and boolean (bit) data.
        Verify that the fist sync sends an activate immediately.
        Verify that the table version is incremented up
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
        BaseTapTest.select_all_streams_and_fields(
            conn_id, found_catalogs, additional_md=additional_md)

        # run a sync and verify exit codes
        sync_job_name_1 = runner.run_sync_mode(self, conn_id)
        exit_status_1 = menagerie.get_exit_status(conn_id, sync_job_name_1)
        menagerie.verify_sync_exit_status(self, exit_status_1, sync_job_name_1)

        # get results of initial sync
        messages_by_stream_1 = runner.get_records_from_target_output()
        initial_state = menagerie.get_state(conn_id)

        # buid out an interrupted state to inject

        # get rowversion for the the midway point in the table
        select_query = f"select rk_col from {database_name}.{schema_name}.{table_name_2} where pk_col = 499;"
        last_fetched_results = mssql_cursor_context_manager(select_query)

        # get rowversion for the max rowin the table
        select_query = f"select rk_col from {database_name}.{schema_name}.{table_name_2} where pk_col = 999;"
        max_results = mssql_cursor_context_manager(select_query)
        max_pk_values = [int(byte) for byte in max_results[0][0]]
        last_pk_fetched = [int(byte) for byte in last_fetched_results[0][0]]

        # set the new state

        # for the interrupted table
        state_to_inject = deepcopy(initial_state)
        state_to_inject['bookmarks'][f'{database_name}_{schema_name}_{table_name_2}']['initial_full_table_complete'] = False
        state_to_inject['bookmarks'][f'{database_name}_{schema_name}_{table_name_2}']['last_pk_fetched'] = {"rk_col": last_pk_fetched}
        state_to_inject['bookmarks'][f'{database_name}_{schema_name}_{table_name_2}']['max_pk_values'] = {"rk_col": max_pk_values}

        # for the not yet synced table
        del state_to_inject['bookmarks'][f'{database_name}_{schema_name}_{table_name_3}']

        menagerie.set_state(conn_id, state_to_inject)

        # run a sync and verify exit codes
        sync_job_name_2 = runner.run_sync_mode(self, conn_id)
        exit_status_2 = menagerie.get_exit_status(conn_id, sync_job_name_2)
        menagerie.verify_sync_exit_status(self, exit_status_2, sync_job_name_2)

        # get results
        messages_by_stream_2 = runner.get_records_from_target_output()
        final_state = menagerie.get_state(conn_id)

        # check the replicated messages
        stream_name_1 = f"{database_name}_{schema_name}_{table_name_1}",
        stream_name_2 = f"{database_name}_{schema_name}_{table_name_2}",
        stream_name_3 = f"{database_name}_{schema_name}_{table_name_3}",

        # verify the already synced table sends only an activate version message
        self.assertIn(stream_name_1, messages_by_stream_2.keys())
        table_1_final_messages = messages_by_stream_2[stream_name_1]['messages']
        self.assertEqual(1, len(table_1_final_messagesmessages_1))
        self.assertEqual('activate_version', table_1_final_messages[0]['action'])

        # verify the yet-to-be-synced table replicates all record messages
        self.assertIn(stream_name_3, messages_by_stream_2.keys())
        table_3_initial_messages = messages_by_stream_2[stream_name_3]['messages']
        table_3_final_messages = messages_by_stream_2[stream_name_3]['messages']
        self.assertEqual(len(table_3_initial_messages), len(table_3_final_messages))

        # verify the interrupted table replicates all record messages from the
        # bookmarked last_pk_fetched through the max_pk_values
        self.assertIn(stream_name_2, messages_by_stream_2.keys())
        table_2_final_messages_pk_values = [message['pk_col']
                                            for message in messages_by_stream_2[stream_name_2]['messages']
                                            if message['action'] == 'upsert']
        self.assertEqual(table_2_final_messages_pk_values[0], sorted(table_2_final_messages_pk_values)[0])
        self.assertEqual(table_2_final_messages_pk_values[-1], sorted(table_2_final_messages_pk_values)[-1])
        expected_primary_key_values = [i for i in range(500)]
        self.assertEqual(expected_primary_key_values, table_2_final_messages_pk_values)

        # check the saved state

        # verify all tables have completed the initial full table sync
        self.assertTrue(final_state['bookmarks'][stream_name_1]['initial_full_table_complete'])
        self.assertTrue(final_state['bookmarks'][stream_name_2]['initial_full_table_complete'])
        self.assertTrue(final_state['bookmarks'][stream_name_3]['initial_full_table_complete'])

        # verify all tables have the same table versions from the first sync
        self.assertEqual(initial_state['bookmarks'][stream_name_1]['version'],
                         final_state['bookmarks'][stream_name_1]['version'])
        self.assertEqual(initial_state['bookmarks'][stream_name_2]['version'],
                         final_state['bookmarks'][stream_name_2]['version'])
        self.assertEqual(initial_state['bookmarks'][stream_name_3]['version'],
                         final_state['bookmarks'][stream_name_3]['version'])

        # verify all tables have a current_log_version saved in state
        self.assertIsInstance(final_state['bookmarks'][stream_name_1]['current_log_version'], int)
        self.assertIsInstance(final_state['bookmarks'][stream_name_2]['current_log_version'], int)
        self.assertIsInstance(final_state['bookmarks'][stream_name_3]['current_log_version'], int)
