"""
Test tap handling of an invalid Object ID in stored in the CHANGE_TRACKING_MIN_VALID_VERSION
"""
import copy

from datetime import datetime, timedelta

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking, update_by_pk, delete_by_pk

from base import BaseTapTest


database_name = "bad_object_id"
schema_name = "dbo"
table_name_1 = "table_one"
table_name_2 = "table_two"


class LogBasedInvalidObjectID(BaseTapTest):

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_bad_object_id".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """The expected streams and metadata about the streams"""

        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""

        drop_all_user_databases()

        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        query_list.extend(enable_database_tracking(database_name))

        column_name = ["pk", "MyIntColumn"]
        column_type = ["int", "int"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        values = [(value, value) for value in range(50)]
        query_list.extend(create_table(database_name, schema_name, table_name_1, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name_1, values))

        mssql_cursor_context_manager(*query_list)

        values = [(value, value) for value in range(500)]
        query_list = list(create_table(database_name, schema_name, table_name_2, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name_2, values))
        mssql_cursor_context_manager(*query_list)

    def expected_sync_streams(self):
        return {
            f'{database_name}_{schema_name}_{table_name_1}',
            f'{database_name}_{schema_name}_{table_name_2}',
        }

    def expected_primary_keys(self):
        return {
            f'{database_name}_{schema_name}_{table_name_1}': {'pk'},
            f'{database_name}_{schema_name}_{table_name_2}': {'pk'},
        }

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
        self.select_all_streams_and_fields(
            conn_id, found_catalogs, additional_md=additional_md)

        # run a sync and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # gather results
        records_by_stream = runner.get_records_from_target_output()
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_primary_keys())
        initial_state = menagerie.get_state(conn_id)
        # query = delete_by_pk(database_name, schema_name, table_name_1,
        #                      [(i,) for i in range(50)], ['pk',])
        # mssql_cursor_context_manager(*query)
        interrupted_state = copy.deepcopy(initial_state)


        import ipdb; ipdb.set_trace()
        1+1

        # run second sync and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # gather results
        records_by_stream = runner.get_records_from_target_output()
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, expected_sync_streams, self.expected_primary_keys())
        final_state = menagerie.get_state(conn_id)
