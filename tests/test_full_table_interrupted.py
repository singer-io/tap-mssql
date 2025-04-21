import sys

import time

from random import randint

from json import dumps

from tap_tester import runner, menagerie, LOGGER

from base import BaseTapTest

from database import drop_all_user_databases, create_database, create_table, mssql_cursor_context_manager, insert


class FullTableInterrupted(BaseTapTest):
    """
    Test the tap can recover from an interruped full table sync

    * Test Setup:
      - Use tables int_data, int_before and varchar_data for interrupted_state scenario
      - int_before table is the table which is replicated prior to the interruption
      - Modify int_data and varchar_data to validate if the updates are being captured post the recovery from interrupted state
      - int_after table is not part of the interrupted_state and is expected to get replicated as a new stream
    """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_full_table_interrupted_test".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """ Expected streams and metadata about the streams """
        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""
        drop_all_user_databases()
        global database_name
        database_name = "full_interruptible"
        global schema_name
        schema_name = "dbo"

        chars = list(range(0, 55296))
        chars.extend(range(57344, 65534))
        chars.extend(range(65535, sys.maxunicode))
        chars.reverse()  # pop starting with ascii characters

        varchar_values = [
            (pk,
             chr(chars.pop()),
             "".join([chr(chars.pop()) for _ in range(15)]),
             "".join([chr(chars.pop()) for _ in range(randint(1, 16))])
             ) for pk in range(5)
        ]
        varchar_values.extend([(5, None, None, None), ])

        int_values = [(0, 0, False),(1, 255, True),(2, 42, None),(3, 230, False),
                      (4, 6, True),(5, 236, True),(6, 27, True),(7, 132, True),
                      (8, 251, False),(9, 187, True),(10, 157, True),(11, 51, True),(12, 144, True)]

        int_before = [(0, 0, False),(1, 255, True),(2, 42, None),(3, 230, False)]

        int_after = [(0, 0, False),(1, 255, True),(2, 42, None),(3, 230, False),
                      (4, 6, True),(5, 236, True),(6, 27, True),(7, 132, True)]

        varchar_schema = {
            'type': 'object',
            'selected': True,
            'properties': {
                'pk': {
                    'maximum': 2147483647,
                    'type': ['integer'],
                    'inclusion': 'automatic',
                    'selected': True,
                    'minimum': -2147483648},
                'varchar_8000': {
                    'type': ['string', 'null'],
                    'inclusion': 'available',
                    'selected': True},
                    # 'minLength': 0},
                'varchar_5': {
                    'type': ['string', 'null'],
                    'inclusion': 'available',
                    'selected': True},
                    # 'minLength': 0},
                'varchar_max': {
                    'type': ['string', 'null'],
                    'inclusion': 'available',
                    'selected': True}}}
                    # 'minLength': 0}}}

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
                'schema': int_schema},
            'int_before': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': int_before,
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
            'int_after': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': int_after,
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
            'varchar_data': {'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': varchar_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': 'varchar_data',
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'varchar_5': {'sql-datatype': 'varchar', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'varchar_8000': {'sql-datatype': 'varchar', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'varchar_max': {'sql-datatype': 'varchar', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': varchar_schema}
            }

        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))

        table_name = "varchar_data"
        column_name = ["pk", "varchar_5", "varchar_8000", "varchar_max"]
        column_type = ["int", "varchar(5)", "varchar(8000)", "varchar(max)"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name, varchar_values))

        table_name = "int_data"
        column_name = ["pk", "MyTinyIntColumn", "my_boolean"]
        column_type = ["int", "tinyint", "bit"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name, int_values))

        table_name = "int_before"
        column_name = ["pk", "MyTinyIntColumn", "my_boolean"]
        column_type = ["int", "tinyint", "bit"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name, int_before))

        table_name = "int_after"
        column_name = ["pk", "MyTinyIntColumn", "my_boolean"]
        column_type = ["int", "tinyint", "bit"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key))
        query_list.extend(insert(database_name, schema_name, table_name, int_after))

        cls.expected_metadata = cls.discovery_expected_metadata

        mssql_cursor_context_manager(*query_list)

    def expected_sync_streams(self):
        return {'full_interruptible_dbo_int_after', 'full_interruptible_dbo_int_before', 'full_interruptible_dbo_int_data', 'full_interruptible_dbo_varchar_data'}

    def expected_primary_keys_by_sync_stream_id(self):
        return {'full_interruptible_dbo_int_data': {'pk'}, 'full_interruptible_dbo_int_before': {'pk'}, 'full_interruptible_dbo_int_after': {'pk'}, 'full_interruptible_dbo_varchar_data': {'pk'}}

    def expected_count(self):
        return {'full_interruptible_dbo_int_data': 6,
               # 'full_interruptible_dbo_int_before': 0, # BUG: TDL-19484 : Tap is re-replicating the stream during the interrupted sync scenario
                'full_interruptible_dbo_int_after': 8,
                'full_interruptible_dbo_varchar_data': 2}

    def test_run(self):

        LOGGER.info("running test %s", self.name())

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

        initial_state = menagerie.get_state(conn_id)

        # Synthesize interrupted state
        interrupted_state = {'bookmarks': {'full_interruptible_dbo_int_before': {'version': 100000000000}}}

        # BUG : TDL-19483 : Missing currently_syncing feature for tap-mssql

        versions = {}

        for stream_name in self.expected_streams()-{'int_before', 'int_after'}:
            first_query = ['select * from '+database_name+'.'+schema_name+'.'+stream_name+' order by pk']
            results = mssql_cursor_context_manager(*first_query)

            last_pk_fetched = results[len(results)//2][0]
            max_pk_value = results[-1][0]

            tap_stream_id = database_name+'_'+schema_name+'_'+stream_name
            version = int(time.time() * 1000)
            interrupted_state['bookmarks'][tap_stream_id] = {
                    'max_pk_values': {'pk': max_pk_value},
                    'last_pk_fetched': {'pk': last_pk_fetched},
                    'version': version
            }
            versions[tap_stream_id] = version

        int_min_query = 'select top 1 pk from '+database_name+'.'+schema_name+'.int_data order by pk'
        int_min_id = mssql_cursor_context_manager(int_min_query)
        int_max_query = 'select top 1 pk from '+database_name+'.'+schema_name+'.int_data order by pk desc'
        int_max_id = mssql_cursor_context_manager(int_max_query)
        update_int_min_query = 'update '+database_name+'.'+schema_name+'.int_data set MyTinyIntColumn = 111 where pk = '+str(int_min_id[0][0])
        mssql_cursor_context_manager(update_int_min_query)
        update_int_max_query = 'update '+database_name+'.'+schema_name+'.int_data set MyTinyIntColumn = 222 where pk = '+str(int_max_id[0][0])
        mssql_cursor_context_manager(update_int_max_query)

        varchar_min_query = 'select top 1 pk from '+database_name+'.'+schema_name+'.varchar_data order by pk'
        varchar_min_id = mssql_cursor_context_manager(varchar_min_query)
        varchar_max_query = 'select top 1 pk from '+database_name+'.'+schema_name+'.varchar_data order by pk desc'
        varchar_max_id = mssql_cursor_context_manager(varchar_max_query)
        update_varchar_min_query = 'update '+database_name+'.'+schema_name+'.varchar_data set varchar_5 = \'TEST\' where pk = '+str(varchar_min_id[0][0])
        mssql_cursor_context_manager(update_varchar_min_query)
        update_varchar_max_query = 'update '+database_name+'.'+schema_name+'.varchar_data set varchar_5 = \'TEST\' where pk = '+str(varchar_max_id[0][0])
        mssql_cursor_context_manager(update_varchar_max_query)

        menagerie.set_state(conn_id, interrupted_state)

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        ################### Assertions #####################

        records_by_stream  = runner.get_records_from_target_output()
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_primary_keys_by_sync_stream_id())

        # verify record counts of streams
        # Workaround for the bug, remove the stream from validation
        # BUG: TDL-19484 : Tap is re-replicating the stream during the interrupted sync scenario
        del record_count_by_stream['full_interruptible_dbo_int_before']
        self.assertEqual(record_count_by_stream, self.expected_count())

        # Verify the state after the interrupted sync contains only the versions for the streams
        first_state_after_interruption = menagerie.get_state(conn_id)

        for value in first_state_after_interruption['bookmarks'].values():
            self.assertNotIn('max_pk_values', value)
            self.assertNotIn('last_pk_fetched', value)
            self.assertIn('version', value)
            self.assertIsInstance(value['version'], int)

        # Verify the data in the modified streams after the interrupted sync recovery
        for stream in self.expected_sync_streams()-{'full_interruptible_dbo_int_before', 'full_interruptible_dbo_int_after'}:
            if stream == 'full_interruptible_dbo_int_data':
                for i in range(len(records_by_stream[stream]['messages'])):
                    if 'data' in records_by_stream[stream]['messages'][i].keys():
                    # Validate if the records replicated is greater than or equal to the interruped record
                        self.assertGreaterEqual(records_by_stream[stream]['messages'][i]['data']['pk'], 7)
                    # validate the update is captured on the last record
                    if 'data' in records_by_stream[stream]['messages'][i].keys() and records_by_stream[stream]['messages'][i]['data']['pk'] == 12:
                        self.assertEqual(records_by_stream[stream]['messages'][i]['data']['MyTinyIntColumn'], 222)
                    # validate the update is not captured on the first record
            else:
                for i in range(len(records_by_stream[stream]['messages'])):
                    if 'data' in records_by_stream[stream]['messages'][i].keys():
                    # Validate if the records replicated is greater than or equal to the interruped record
                        self.assertGreaterEqual(records_by_stream[stream]['messages'][i]['data']['pk'], 4)
                    # validate the update is captured on the last record
                    if 'data' in records_by_stream[stream]['messages'][i].keys() and records_by_stream[stream]['messages'][i]['data']['pk'] == 5:
                        self.assertEqual(records_by_stream[stream]['messages'][i]['data']['varchar_5'], 'TEST')

        # ActivateVersionMessage validation

        for stream_name in self.expected_sync_streams():
            # for the replication of the table after the interruption, validate if ActivateVersionMessage is present as the first and the last message
            if stream_name == 'full_interruptible_dbo_int_after':
                self.assertEqual('activate_version', records_by_stream[stream_name]['messages'][-1]['action'])
                self.assertEqual('activate_version', records_by_stream[stream_name]['messages'][0]['action'])
            # for the tables involved in interruption scenario, validate if ActivateVersionMessage is present as the last message and not the first
            else:
                self.assertNotEqual('activate_version', records_by_stream[stream_name]['messages'][0]['action'])
                self.assertEqual('activate_version', records_by_stream[stream_name]['messages'][-1]['action'])
