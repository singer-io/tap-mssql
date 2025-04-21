"""
Test log based interrupted replication
"""
import copy

from datetime import datetime, timedelta

from tap_tester import menagerie, runner, LOGGER

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking, update_by_pk, delete_by_pk

from base import BaseTapTest


class LogBasedInterrupted(BaseTapTest):

    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_log_based_interrupted_test".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """The expected streams and metadata about the streams"""

        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""
        global database_name
        global schema_name
        database_name = "log_based_interruptible"
        schema_name = "dbo"

        cls.EXPECTED_METADATA = {
            'log_based_interruptible_dbo_int_data': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': [
                    (0, -9223372036854775808, -2147483648, -32768),
                    (1, 0, 0, 0),
                    (2, 9223372036854775807, 2147483647, 32767),
                    (3, None, None, None),
                    (4, 5603121835631323156, 9665315, 11742),
                    (5, -4898597031243117659, 140946744, -16490),
                    (6, -5168593529138936444, -1746890910, 2150),
                    (7, 1331162887494168851, 1048867088, 12136),
                    (8, -4495110645908459596, -1971955745, 18257),
                    (9, -1575653240237191360, -533282078, 22022),
                    (10, 6203877631305833079, 271324086, -18782),
                    (11, 7293147954924079156, 1003163272, 3593),
                    (12, -1302715001442736465, -1626372079, 3788),
                    (13, -9062593720232233398, 1646478731, 17621)],
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': 'int_data',
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'MyBigIntColumn': {'sql-datatype': 'bigint', 'selected-by-default': True,
                                        'inclusion': 'available'}},
                    {'MyIntColumn': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'MySmallIntColumn': {'sql-datatype': 'smallint', 'selected-by-default': True,
                                          'inclusion': 'available'}}],
                'schema': {
                    'type': 'object',
                    'properties': {
                        'MySmallIntColumn': {
                            'type': ['integer', 'null'],
                            'minimum': -32768,
                            'maximum': 32767,
                            'inclusion': 'available',
                            'selected': True},
                        'pk':
                            {'type': ['integer'],
                             'minimum': -2147483648,
                             'maximum': 2147483647,
                             'inclusion': 'automatic',
                             'selected': True},
                        'MyBigIntColumn': {
                            'type': ['integer', 'null'],
                            'minimum': -9223372036854775808,
                            'maximum': 9223372036854775807,
                            'inclusion': 'available',
                            'selected': True},
                        'MyIntColumn': {
                            'type': ['integer', 'null'],
                            'minimum': -2147483648,
                            'maximum': 2147483647,
                            'inclusion': 'available',
                            'selected': True},
                        "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}},
                    'selected': True}},
            'log_based_interruptible_dbo_int_and_bool_data': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': [
                    (0, 0, False),
                    (1, 255, True),
                    (2, None, None),
                    (3, 230, False),
                    (4, 6, True),
                    (5, 236, True),
                    (6, 27, True),
                    (7, 132, True),
                    (8, 251, False),
                    (9, 187, True),
                    (10, 157, True),
                    (11, 51, True),
                    (12, 144, True)],
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': 'int_and_bool_data',
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'MyTinyIntColumn': {'sql-datatype': 'tinyint', 'selected-by-default': True,
                                         'inclusion': 'available'}},
                    {'my_boolean': {'sql-datatype': 'bit', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': {
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
                            'selected': True},
                        "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}},
                    'selected': True}}}

        drop_all_user_databases()

        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        query_list.extend(enable_database_tracking(database_name))

        table_name = "int_data"
        column_name = ["pk", "MyBigIntColumn", "MyIntColumn", "MySmallIntColumn"]
        column_type = ["int", "bigint", "int", "smallint"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["log_based_interruptible_dbo_int_data"]["values"]))

        table_name = "int_and_bool_data"
        column_name = ["pk", "MyTinyIntColumn", "my_boolean"]
        column_type = ["int", "tinyint", "bit"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["log_based_interruptible_dbo_int_and_bool_data"]["values"]))

        mssql_cursor_context_manager(*query_list)
        cls.expected_metadata = cls.discovery_expected_metadata

    def expected_sync_streams(self):
        return {'log_based_interruptible_dbo_int_data', 'log_based_interruptible_dbo_int_and_bool_data'}

    def expected_count(self):
        return {'log_based_interruptible_dbo_int_and_bool_data': 13,
                'log_based_interruptible_dbo_int_data': 14}

    def expected_primary_keys_by_sync_stream_id(self):
        return {'log_based_interruptible_dbo_int_data': {'pk'},
                'log_based_interruptible_dbo_int_and_bool_data': {'pk'}}

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
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify records match on the first sync
        records_by_stream = runner.get_records_from_target_output()

        pk_count_by_stream = self.unique_pk_count_by_stream(records_by_stream)
        self.assertEqual(pk_count_by_stream, self.expected_count())

        table_version = dict()
        initial_log_version = dict()
        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):
                stream_expected_data = self.expected_metadata()[stream]
                table_version[stream] = records_by_stream[stream]['table_version']

                # gather all actions then verify 3 activate versions, 1 at start, 2 in the last 3
                actions = [rec['action'] for rec in records_by_stream[stream]['messages']]
                self.assertEqual(actions[0], 'activate_version')
                self.assertEqual(len([a for a in actions[-3:] if a == "activate_version"]), 2,
                    msg=("Expected 2 of the last 3 messages to be activate version messages. 1 for "
                         "end of full table and 1 for beginning of log based. Position can vary "
                         "due to TDL-24162")
                )

                # verify state and bookmarks
                initial_state = menagerie.get_state(conn_id)
                bookmark = initial_state['bookmarks'][stream]

                self.assertIsNone(initial_state.get('currently_syncing'),
                                  msg="expected state's currently_syncing to be None")
                self.assertIsNotNone(bookmark.get('current_log_version'),
                    msg="expected bookmark to have current_log_version due to log replication")
                self.assertTrue(bookmark['initial_full_table_complete'],
                                msg="expected full table to be complete")
                inital_log_version = bookmark['current_log_version']

                self.assertEqual(bookmark['version'], table_version[stream],
                                 msg="expected bookmark for stream to match version")

                expected_schemas = self.expected_metadata()[stream]['schema']
                self.assertEqual(records_by_stream[stream]['schema'], expected_schemas,
                                 msg="expected: {} != actual: {}".format(
                                     expected_schemas, records_by_stream[stream]['schema']))
                initial_log_version[stream] = bookmark['current_log_version']

        initial_log_version_value = set(initial_log_version.values()).pop()

        #  Overwrite state to mimic an interupted sync on table_interrupted

        interrupted_state = copy.deepcopy(initial_state)

        # modify the state to simulate :
        # --> A table exists in bookmark
        # --> A table which is interrupted

        del interrupted_state['bookmarks']['log_based_interruptible_dbo_int_data']['version']
        interrupted_state['bookmarks']['log_based_interruptible_dbo_int_data'][
            'initial_full_table_complete'] = False

        max_pk_values = {'max_pk_values': {'pk': 12}}
        last_pk_fetched = {'last_pk_fetched': {'pk': 10}}

        interrupted_state['bookmarks']['log_based_interruptible_dbo_int_and_bool_data'].update(
            max_pk_values)
        interrupted_state['bookmarks']['log_based_interruptible_dbo_int_and_bool_data'].update(
            last_pk_fetched)
        interrupted_state['bookmarks']['log_based_interruptible_dbo_int_and_bool_data'][
            'initial_full_table_complete'] = False

        menagerie.set_state(conn_id, interrupted_state)

        # create a new table between syncs to cover case where table does not exist in state
        query_list = []
        table_name = "int_data_after"
        column_name = ["pk", "MyIntColumn", "MySmallIntColumn"]
        column_type = ["int", "int", "smallint"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        int_after_values = [(0, 22, 44),
                    (1, 0, 0),
                    (2, 23, 43),
                    (3, None, None),
                    (4, 24, 44),
                    (5, 25, 45)]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 int_after_values))

        mssql_cursor_context_manager(*query_list)

        # add new table's pk to expected_metadata
        self.EXPECTED_METADATA['log_based_interruptible_dbo_int_data_after'] = {
            self.PRIMARY_KEYS: {'pk'}}

        # invoke the sync job AGAIN following various manipulations to the data

        # add the newly created stream in the expectations
        expected_sync_streams = self.expected_sync_streams()
        expected_sync_streams.add('log_based_interruptible_dbo_int_data_after')
        expected_primary_keys_by_sync_stream_id = self.expected_primary_keys_by_sync_stream_id()
        expected_primary_keys_by_sync_stream_id[
            'log_based_interruptible_dbo_int_data_after'] = {'pk'}
        expected_count = self.expected_count()
        expected_count['log_based_interruptible_dbo_int_data_after'] = 6
        expected_count['log_based_interruptible_dbo_int_and_bool_data'] = 2
        expected_count['log_based_interruptible_dbo_int_data'] = 14

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

        # run second sync and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        records_by_stream = runner.get_records_from_target_output()

        pk_count_by_stream = self.unique_pk_count_by_stream(records_by_stream)

        second_state = menagerie.get_state(conn_id)
        bookmark_2 = second_state['bookmarks']

        # validate the record count for all the streams after interruption recovery, use unique
        #   pks instead of all upserts to de-dupe and avoid inconsistency from TDL-24162
        self.assertEqual(pk_count_by_stream, expected_count)

        second_log_version = dict()
        for stream in expected_sync_streams:
            with self.subTest(stream=stream):

                table_bookmark = bookmark_2[stream]
                # validate the state is not having max_pk_values and last_fetched_pk
                self.assertTrue(table_bookmark['initial_full_table_complete'])
                self.assertNotIn('max_pk_values', table_bookmark.keys())
                self.assertNotIn('last_pk_fetched', table_bookmark.keys())

                second_log_version[stream] = table_bookmark['current_log_version']

                # verify if the data is replicated accurately post the interruption
                if stream == 'log_based_interruptible_dbo_int_and_bool_data':
                    for rec in records_by_stream[stream]['messages']:
                        if rec['action'] == 'upsert':
                            self.assertIn(rec['data']['pk'], [11, 12])

        second_log_version_value = set(second_log_version.values()).pop()

        # validate the current log version increment
        self.assertGreater(second_log_version_value, initial_log_version_value)

        #insert data to the third table
        query_list = []
        int_after_second_insert = [(6, 66, 444),
                                   (7, 77, 777)]
        query_list.extend(insert(database_name, schema_name, table_name,
                                 int_after_second_insert))

        mssql_cursor_context_manager(*query_list)

        #### run third sync ####

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

        # run second sync and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        records_by_stream = runner.get_records_from_target_output()

        pk_count_by_stream = self.unique_pk_count_by_stream(records_by_stream)

        expected_count['log_based_interruptible_dbo_int_data_after'] = 3
        expected_count['log_based_interruptible_dbo_int_and_bool_data'] = 0
        expected_count['log_based_interruptible_dbo_int_data'] = 0

        self.assertEqual(pk_count_by_stream, expected_count)

        final_state = menagerie.get_state(conn_id)
        bookmark_3 = final_state['bookmarks']

        final_log_version = dict()
        for stream in expected_sync_streams:
            with self.subTest(stream=stream):

                table_bookmark = bookmark_3[stream]
                # validate the state is not having max_pk_values and last_fetched_pk
                self.assertTrue(table_bookmark['initial_full_table_complete'])
                self.assertNotIn('max_pk_values', table_bookmark.keys())
                self.assertNotIn('last_pk_fetched', table_bookmark.keys())

                final_log_version[stream] = table_bookmark['current_log_version']

                # verify if the data is replicated accurately post the interruption
                if stream == 'log_based_interruptible_dbo_int_data_after':
                    for rec in records_by_stream[stream]['messages']:
                        if rec['action'] == 'upsert':
                            self.assertIn(rec['data']['pk'], [5, 6, 7])

        final_log_version_value = set(final_log_version.values()).pop()

        # validate the current log version increment
        self.assertGreater(final_log_version_value, second_log_version_value)
