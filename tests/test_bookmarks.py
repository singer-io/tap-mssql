"""
Test tap sets a bookmark and respects it for the next sync of a stream
"""
from datetime import datetime as dt
from dateutil.parser import parse

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking, update_by_pk, delete_by_pk
from base import BaseTapTest


class BookmarkTest(BaseTapTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_bookmark_test".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """The expected streams and metadata about the streams"""

        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""

        database_name = "data_types_database"
        schema_name = "dbo"

        cls.EXPECTED_METADATA = {
            'data_types_database_dbo_integers': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': [
                    (0, -9223372036854775808, -2147483648, -32768),
                    (1, 0, 0, 0),
                    (2, 9223372036854775805, 2147483647, 32767),
                    (3, 42, None, None), # TODO This was None before, is that something that should even be handled?
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
                'stream_name': 'integers',
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'replication_key_column': {'sql-datatype': 'bigint', 'selected-by-default': True,
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
                        'replication_key_column': {
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
                            'selected': True}},
                    'selected': True}},
            'data_types_database_dbo_tiny_integers_and_bools': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': [
                    (0, 0, False),
                    (1, 253, True),
                    (2, 255, None),
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
                'stream_name': 'tiny_integers_and_bools',
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'replication_key_column': {'sql-datatype': 'tinyint', 'selected-by-default': True,
                                         'inclusion': 'available'}},
                    {'my_boolean': {'sql-datatype': 'bit', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': {
                    'type': 'object',
                    'properties': {
                        'replication_key_column': {
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
                    'selected': True}}}

        drop_all_user_databases()

        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        query_list.extend(enable_database_tracking(database_name))

        table_name = "integers"
        column_name = ["pk", "replication_key_column", "MyIntColumn", "MySmallIntColumn"]
        column_type = ["int", "bigint", "int", "smallint"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["data_types_database_dbo_integers"]["values"]))

        table_name = "tiny_integers_and_bools"
        column_name = ["pk", "replication_key_column", "my_boolean"]
        column_type = ["int", "tinyint", "bit"]
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=True))
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["data_types_database_dbo_tiny_integers_and_bools"]["values"]))

        mssql_cursor_context_manager(*query_list)
        cls.expected_metadata = cls.discovery_expected_metadata

    def test_run(self):
        """
        Verify that for each stream you can do a sync which records bookmarks.
        That the bookmark is the maximum value sent to the target for the replication key.
        That a second sync respects the bookmark
            All data of the second sync is >= the bookmark from the first sync
            The number of records in the 2nd sync is less then the first (This assumes that
                new data added to the stream is done at a rate slow enough that you haven't
                doubled the amount of data from the start date to the first sync between
                the first sync and second sync run in this test)

        Verify that only data for incremental streams is sent to the target

        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """

        print("running test {}".format(self.name()))

        conn_id = self.create_connection()

        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL}

        # IF THERE ARE STREAMS THAT SHOULD NOT BE TESTED
        # REPLACE THE EMPTY SET BELOW WITH THOSE STREAMS
        untested_streams = self.child_streams().union(set())
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams.difference(
                            untested_streams)]
        self.select_all_streams_and_fields(conn_id, our_catalogs)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()),
                         incremental_streams.difference(untested_streams))

        first_sync_state = menagerie.get_state(conn_id)

        # Get data about actual rows synced
        first_sync_records = runner.get_records_from_target_output()
        first_max_bookmarks = self.max_bookmarks_by_stream(first_sync_records)
        first_min_bookmarks = self.min_bookmarks_by_stream(first_sync_records)

        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)

        # Get data about rows synced
        second_sync_records = runner.get_records_from_target_output()
        second_min_bookmarks = self.min_bookmarks_by_stream(second_sync_records)

        # THIS MAKES AN ASSUMPTION THAT CHILD STREAMS DO NOT HAVE BOOKMARKS.
        # ADJUST IF NECESSARY
        for stream in incremental_streams.difference(untested_streams):
            with self.subTest(stream=stream):

                # get bookmark values from state and target data
                stream_bookmark_key = self.expected_replication_keys().get(stream, set())
                assert len(
                    stream_bookmark_key) == 1  # There shouldn't be a compound replication key
                stream_bookmark_key = stream_bookmark_key.pop()

                state_value = first_sync_state.get("bookmarks", {}).get(
                    stream, {None: None}).get(stream_bookmark_key)
                target_value = first_max_bookmarks.get(
                    stream, {None: None}).get(stream_bookmark_key)
                target_min_value = first_min_bookmarks.get(
                    stream, {None: None}).get(stream_bookmark_key)

                try:
                    # attempt to parse the bookmark as a date
                    if state_value:
                        if isinstance(state_value, str):
                            state_value = self.local_to_utc(parse(state_value))
                        if isinstance(state_value, int):
                            state_value = self.local_to_utc(dt.utcfromtimestamp(state_value))

                    if target_value:
                        if isinstance(target_value, str):
                            target_value = self.local_to_utc(parse(target_value))
                        if isinstance(target_value, int):
                            target_value = self.local_to_utc(dt.utcfromtimestamp(target_value))

                    if target_min_value:
                        if isinstance(target_min_value, str):
                            target_min_value = self.local_to_utc(parse(target_min_value))
                        if isinstance(target_min_value, int):
                            target_min_value = self.local_to_utc(
                                dt.utcfromtimestamp(target_min_value))

                except (OverflowError, ValueError, TypeError):
                    print("bookmarks cannot be converted to dates, comparing values directly")

                # verify that there is data with different bookmark values - setup necessary
                self.assertGreaterEqual(target_value, target_min_value,
                                        msg="Data isn't set up to be able to test bookmarks")

                # verify state agrees with target data after 1st sync
                self.assertEqual(state_value, target_value,
                                 msg="The bookmark value isn't correct based on target data")

                # verify that you get less data the 2nd time around
                self.assertGreater(
                    first_sync_record_count.get(stream, 0),
                    second_sync_record_count.get(stream, 0),
                    msg="second syc didn't have less records, bookmark usage not verified")

                # verify all data from 2nd sync >= 1st bookmark
                target_value = second_min_bookmarks.get(
                    stream, {None: None}).get(stream_bookmark_key)
                try:
                    if target_value:
                        if isinstance(target_value, str):
                            target_value = self.local_to_utc(parse(target_value))
                        if isinstance(target_value, int):
                            target_value = self.local_to_utc(dt.utcfromtimestamp(target_value))

                except (OverflowError, ValueError, TypeError):
                    print("bookmarks cannot be converted to dates, comparing values directly")

                # verify that the minimum bookmark sent to the target for the second sync
                # is greater than or equal to the bookmark from the first sync
                self.assertGreaterEqual(target_value, state_value) # TODO no data changes b/t syncs, this is a bit misleading

                # TODO TEST GOAL | create and update data between syncs
