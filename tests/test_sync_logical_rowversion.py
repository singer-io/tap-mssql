"""
Test tap discovery
"""
from copy import deepcopy
from datetime import datetime, timedelta

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking, update_by_pk, delete_by_pk

from base import BaseTapTest


database_name = "row_version_state"
schema_name = "dbo"
table_name_1 = "row_version_1"  # sync completed
table_name_2 = f"row_version_2"  # sync interrupted
table_name_3 = f"row_version_3"  # sync not started

class SyncIntLogical(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_log_rowversion".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """The expected streams and metadata about the streams"""

        return cls.EXPECTED_METADATA

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
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # buid out an interrupted state to inject
        # get state
        initial_state = menagerie.get_state(conn_id)
        # get rowversion for the the midway point in the table
        select_query = f"select rk_col from {database_name}.{schema_name}.{table_name_2} where pk_col = 500;"
        last_fetched_results = mssql_cursor_context_manager(select_query)
        # get rowversion for the max rowin the table
        select_query = f"select rk_col from {database_name}.{schema_name}.{table_name_2} where pk_col = 999;"
        max_results = mssql_cursor_context_manager(select_query)
        max_pk_values = [int(byte) for byte in bytearray(max_results[0][0])]
        last_pk_fetched = [int(byte) for byte in bytearray(last_fetched_results[0][0])]

        # set the new state
        # for the interrupted table
        state_to_inject = deepcopy(initial_state)
        state_to_inject['bookmarks'][f'{database_name}_{schema_name}_{table_name_2}']['initial_full_table_complete'] = False
        state_to_inject['bookmarks'][f'{database_name}_{schema_name}_{table_name_2}']['last_pk_fetched'] = {"RowVersion": last_pk_fetched}
        state_to_inject['bookmarks'][f'{database_name}_{schema_name}_{table_name_2}']['max_pk_values'] = {"RowVersion": max_pk_values}
        # for the not yet synced table
        state_to_inject['bookmarks'][f'{database_name}_{schema_name}_{table_name_3}']['initial_full_table_complete'] = False
        menagerie.set_state(conn_id, state_to_inject)

        # run a sync and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify records match on the first sync
        records_by_stream = runner.get_records_from_target_output()
