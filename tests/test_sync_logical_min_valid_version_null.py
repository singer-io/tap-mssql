"""
Test tap log sync with min valid version null
"""
from datetime import datetime, timedelta
from decimal import getcontext, Decimal

import simplejson

from tap_tester import menagerie, runner

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager, insert, enable_database_tracking, delete_by_pk, update_by_pk

from base import BaseTapTest

getcontext().prec = 38
DECIMAL_PRECISION_SCALE = [(9, 4), (19, 6), (28, 6), (38, 13)]
NUMERIC_PRECISION_SCALE = [(9, 4), (19, 12), (28, 22), (38, 3)]


class LogSyncMinValidVersionNull(BaseTapTest):
    """ Test the tap log sync with min valid version null """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_log_sync_min_valid_vers_null".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """The expected streams and metadata about the streams"""

        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""

        database_name = "data_types_database"
        schema_name = "dbo"
        drop_all_user_databases()

        numeric_values = [
            (0, Decimal('-99999.9999'), Decimal('-9999999.999999999999'), Decimal('-999999.9999999999999999999999'), Decimal('-99999999999999999999999999999999999.999')),
            (1, 0, 0, 0, 0),
            (2, None, None, None, None),
            (3, Decimal('99999.9999'), Decimal('9999999.999999999999'), Decimal('999999.9999999999999999999999'), Decimal('99999999999999999999999999999999999.999')),
            (4, Decimal('96701.9382'), Decimal('-4371716.186100650268'), Decimal('-367352.306093776232045517794'), Decimal('-81147872128956247517327931319278572.985')),
            (5, Decimal('-73621.9366'), Decimal('2564047.277589545531'), Decimal('336177.4754683699464233786667'), Decimal('46946462608534127558389411015159825.758')),
            (6, Decimal('-3070.7339'), Decimal('6260062.158440967433'), Decimal('-987006.0035971607740533206418'), Decimal('95478671259010046866787754969592794.61'))]

        numeric_schema = {
            'type': 'object',
            'properties': {
                'numeric_9_4': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': 0.0001,
                    'maximum': 1e5,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e5},
                'numeric_19_12': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': 1e-12,
                    'maximum': 1e7,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e7},
                'numeric_28_22': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': 1e-22,
                    'maximum': 1e6,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e6},
                'numeric_38_3': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': .001,
                    'maximum': 1e35,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e35},
                'pk': {
                    'maximum': 2147483647,
                    'type': ['integer'],
                    'inclusion': 'automatic',
                    'minimum': -2147483648,
                    'selected': True},
                "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}},
            'selected': True}

        decimal_values = [
            (0, Decimal('-99999.9999'), Decimal('-9999999999999.999999'), Decimal('-9999999999999999999999.999999'), Decimal('-9999999999999999999999999.9999999999999')),
            (1, 0, 0, 0, 0),
            (2, None, None, None, None),
            (3, Decimal('99999.9999'), Decimal('9999999999999.999999'), Decimal('9999999999999999999999.999999'), Decimal('9999999999999999999999999.9999999999999')),
            (4, Decimal('-92473.8401'), Decimal('-4182159664734.645653'), Decimal('6101329656084900380190.268036'), Decimal('4778017533841887320066645.9761464001349')),
            (5, Decimal('-57970.8157'), Decimal('7735958802279.086687'), Decimal('4848737828398517845540.057905'), Decimal('2176036096567853905237453.5152648989022')),
            (6, Decimal('57573.9037'), Decimal('5948502499261.181557'), Decimal('-6687721783088280707003.076638'), Decimal('-6264019242578746090842245.3746225058202'))]

        decimal_schema = {
            'type': 'object',
            'properties': {
                'decimal_9_4': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': 0.0001,
                    'maximum': 1e5,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e5},
                'decimal_19_6': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': 1e-6,
                    'maximum': 1e13,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e13},
                'decimal_28_6': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': 1e-6,
                    'maximum': 1e22,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e22},
                'decimal_38_13': {
                    'exclusiveMaximum': True,
                    'type': ['number', 'null'],
                    'selected': True,
                    'multipleOf': 1e-13,
                    'maximum': 1e25,
                    'inclusion': 'available',
                    'exclusiveMinimum': True,
                    'minimum': -1e25},
                'pk': {
                    'maximum': 2147483647,
                    'type': ['integer'],
                    'inclusion': 'automatic',
                    'minimum': -2147483648,
                    'selected': True},
                "_sdc_deleted_at": {'format': 'date-time', 'type': ['string', 'null']}},
            'selected': True}

        cls.EXPECTED_METADATA = {
            'data_types_database_dbo_numeric_precisions': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': numeric_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': 'numeric_precisions',
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'numeric_9_4': {'sql-datatype': 'numeric(9,4)', 'selected-by-default': True,
                                        'inclusion': 'available'}},
                    {'numeric_19_12': {'sql-datatype': 'numeric(19,12)', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'numeric_28_22': {'sql-datatype': 'numeric(28,22)', 'selected-by-default': True, 'inclusion': 'available'}},
                    {'numeric_38_3': {'sql-datatype': 'numeric(38,3)', 'selected-by-default': True, 'inclusion': 'available'}}],
                'schema': numeric_schema},
            'data_types_database_dbo_decimal_precisions': {
                'is-view': False,
                'schema-name': schema_name,
                'row-count': 0,
                'values': decimal_values,
                'table-key-properties': {'pk'},
                'selected': None,
                'database-name': database_name,
                'stream_name': 'decimal_precisions',
                'fields': [
                    {'pk': {'sql-datatype': 'int', 'selected-by-default': True, 'inclusion': 'automatic'}},
                    {'decimal_9_4': {'sql-datatype': 'decimal(9,4)', 'selected-by-default': True,
                                     'inclusion': 'available'}},
                    {'decimal_19_6': {'sql-datatype': 'decimal(19,6)', 'selected-by-default': True,
                                      'inclusion': 'available'}},
                    {'decimal_28_6': {'sql-datatype': 'decimal(28,6)', 'selected-by-default': True,
                                       'inclusion': 'available'}},
                    {'decimal_38_13': {'sql-datatype': 'decimal(38,13)', 'selected-by-default': True,
                                      'inclusion': 'available'}}],
                'schema': decimal_schema}}
        query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        #query_list.extend(enable_database_tracking(database_name)) # comment out for null from db case

        # TODO - BUG https://stitchdata.atlassian.net/browse/SRCE-1075
        table_name = "numeric_precisions"
        precision_scale = NUMERIC_PRECISION_SCALE
        column_type = [
            "numeric({},{})".format(precision, scale)
            for precision, scale in precision_scale
        ]
        column_name = ["pk"] + [x.replace("(", "_").replace(",", "_").replace(")", "") for x in column_type]
        column_type = ["int"] + column_type
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=False)) #tracking=False (null from db case)
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["data_types_database_dbo_numeric_precisions"]["values"]))

        table_name = "decimal_precisions"
        precision_scale = DECIMAL_PRECISION_SCALE
        column_type = [
            "decimal({},{})".format(precision, scale)
            for precision, scale in precision_scale
        ]
        column_name = ["pk"] + [x.replace("(", "_").replace(",", "_").replace(")", "") for x in column_type]
        column_type = ["int"] + column_type
        primary_key = {"pk"}
        column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        query_list.extend(create_table(database_name, schema_name, table_name, column_def,
                                       primary_key=primary_key, tracking=False)) #tracking=False (null from db case)
        query_list.extend(insert(database_name, schema_name, table_name,
                                 cls.EXPECTED_METADATA["data_types_database_dbo_decimal_precisions"]["values"]))

        mssql_cursor_context_manager(*query_list)

        cls.expected_metadata = cls.discovery_expected_metadata

    def test_run(self):
        """
        Verify that a log based sync handles a min valid version of null as expected
        """
        print("running test {}".format(self.name()))

        conn_id = self.create_connection() # create connection and run check job

        # get the catalog information of discovery
        found_catalogs = menagerie.get_catalogs(conn_id)
        additional_md = [{"breadcrumb": [], "metadata": {'replication-method': 'LOG_BASED'}}]
        BaseTapTest.select_all_streams_and_fields(
            conn_id, found_catalogs, additional_md=additional_md)

        # clear state
        menagerie.set_state(conn_id, {})

        # run a sync and verify exit codes
        failed_sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, failed_sync_job_name)

        self.assertEqual(exit_status['tap_exit_status'], 1)
        self.assertIn('Change Tracking is not enabled',exit_status['tap_error_message'])

        '''
        TODO add addtional min_valid_version = null cases.  4 cases identified
             1 - null / none value returned from state (current_log_version = None). Covered in
                 test_sync_logical_current_log_version_null.py
             2 - Change tracking is not enabled in the database.  Covered in this test.
             3 - The specified object ID is not valid for the current DB (not covered).
             4 - Insufficient permission to the table specified by the object ID (not covered).

             Generate spike card for uncovered cases 3 and 4.
             https://jira.talendforge.org/browse/TDL-19227
        '''
