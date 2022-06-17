"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import unittest
import os
import backoff
from datetime import datetime as dt
from datetime import timezone as tz

from tap_tester import connections, menagerie, runner

from spec import TapSpec

def backoff_wait_times():
    """Create a generator of wait times as [30, 60, 120, 240, 480, ...]"""
    return backoff.expo(factor=30)

class BaseTapTest(TapSpec, unittest.TestCase):
    """
    Setup expectations for test sub classes
    Run discovery for as a prerequisite for most tests
    """

    @staticmethod
    def name():
        """The name of the test within the suite"""
        return "tap_tester_{}".format(TapSpec.tap_name())

    def environment_variables(self):
        return ({p for p in self.CONFIGURATION_ENVIRONMENT['properties'].values()} |
                {c for c in self.CONFIGURATION_ENVIRONMENT['credentials'].values()})

    def expected_streams(self):
        """A set of expected stream ids"""
        return set(self.expected_metadata().keys())

    def child_streams(self):
        """
        Return a set of streams that are child streams
        based on having foreign key metadata
        """
        return {stream for stream, metadata in self.expected_metadata().items()
                if metadata.get(self.FOREIGN_KEYS)}

    def expected_primary_keys_by_stream_id(self):
        """
        return a dictionary with key of table name (stream_id)
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
                              column_type, primary_key, values=None, view: bool = False):
        column_metadata = cls.expected_column_metadata(cls, column_name, column_type, primary_key)

        data = {
            cls.DATABASE_NAME: database_name,
            cls.SCHEMA: "public" if schema_name is None else schema_name,
            cls.STREAM: table_name,
            cls.VIEW: view,
            cls.PRIMARY_KEYS: primary_key,
            cls.ROWS: 0,
            cls.SELECTED: None,
            cls.FIELDS: column_metadata,
            cls.VALUES: values
        }

        cls.EXPECTED_METADATA["{}_{}_{}".format(
            data[cls.DATABASE_NAME],
            data[cls.SCHEMA],
            data[cls.STREAM])] = data

    def expected_column_metadata(self, column_name, column_type, primary_key):
        column_metadata = [{x[0]: {self.DATATYPE: x[1]}} for x in list(zip(column_name, column_type))]
        # primary keys have inclusion of automatic
        for field in column_metadata:
            if set(field.keys()).intersection(primary_key):
                field[list(field.keys())[0]][self.INCLUSION] = self.AUTOMATIC_FIELDS
                field[list(field.keys())[0]][self.DEFAULT_SELECT] = True

        # other fields are available if supported otherwise unavailable (unsupported)
        for field in column_metadata:
            if not set(field.keys()).intersection(primary_key):
                if field[list(field.keys())[0]][self.DATATYPE] in self.SUPPORTED_DATATYPES:
                    field[list(field.keys())[0]][self.INCLUSION] = self.AVAILABLE_FIELDS
                    field[list(field.keys())[0]][self.DEFAULT_SELECT] = True
                else:
                    field[list(field.keys())[0]][self.INCLUSION] = self.UNAVAILABLE_FIELDS
                    field[list(field.keys())[0]][self.DEFAULT_SELECT] = False

        # float's and real's don't keep there precision and 24 or less floats are actually reals
        for field in column_metadata:
            datatype = field[list(field.keys())[0]][self.DATATYPE]
            index = datatype.find("(")
            if index > -1:
                if datatype[:index] == "float":
                    if int(datatype[index+1:-1]) <= 24:
                        field[list(field.keys())[0]][self.DATATYPE] = "real"
                    else:
                        field[list(field.keys())[0]][self.DATATYPE] = "float"

        # rowversion shows up as type timestamp, they are synonyms
        for field in column_metadata:
            datatype = field[list(field.keys())[0]][self.DATATYPE]
            if datatype == "rowversion":
                field[list(field.keys())[0]][self.DATATYPE] = "timestamp"

        # TODO - BUG - Remove this if we determine sql-datatypes should include precision/scale
        for field in column_metadata:
            datatype = field[list(field.keys())[0]][self.DATATYPE]
            index = datatype.find("(")
            if index > -1:  # and "numeric" not in datatype and "decimal" not in datatype:
                field[list(field.keys())[0]][self.DATATYPE] = datatype[:index]
        return column_metadata

    def expected_foreign_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of foreign key fields
        """
        return {table: properties.get(self.FOREIGN_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_method(self):
        """return a dictionary with key of table name nd value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def setUp(self):
        """Verify that you have set the prerequisites to run the tap (creds, etc.)"""
        missing_envs = [x for x in self.environment_variables() if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Missing test-required environment variables: {}".format(missing_envs))

    #########################
    #   Helper Methods      #
    #########################

    def create_connection(self, original_properties: bool = True):
        """Create a new connection with the test name"""

        # Create the connection
        conn_id = connections.ensure_connection(self, original_properties)

        # Run a check job using orchestrator (discovery)
        check_job_name = runner.run_check_mode(self, conn_id)

        # Assert that the check job succeeded
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)
        return conn_id

    def run_sync(self, conn_id, clear_state=False):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        if clear_state:
            menagerie.set_state(conn_id, {})

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys_by_stream_id())
        return sync_record_count

    @staticmethod
    def local_to_utc(date: dt):
        """Convert a datetime with timezone information to utc"""
        utc = dt(date.year, date.month, date.day, date.hour, date.minute,
                 date.second, date.microsecond, tz.utc)

        if date.tzinfo and hasattr(date.tzinfo, "_offset"):
            utc += date.tzinfo._offset

        return utc

    def max_bookmarks_by_stream(self, sync_records):
        """
        Return the maximum value for the replication key for each stream
        which is the bookmark expected value.

        Comparisons are based on the class of the bookmark value. Dates will be
        string compared which works for ISO date-time strings
        """
        max_bookmarks = {}
        for stream, batch in sync_records.items():

            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']
            stream_bookmark_key = self.expected_replication_keys().get(stream, set())
            assert len(stream_bookmark_key) == 1  # There shouldn't be a compound replication key
            stream_bookmark_key = stream_bookmark_key.pop()

            bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]
            max_bookmarks[stream] = {stream_bookmark_key: None}
            for bk_value in bk_values:
                if bk_value is None:
                    continue

                if max_bookmarks[stream][stream_bookmark_key] is None:
                    max_bookmarks[stream][stream_bookmark_key] = bk_value

                if bk_value > max_bookmarks[stream][stream_bookmark_key]:
                    max_bookmarks[stream][stream_bookmark_key] = bk_value
        return max_bookmarks

    def min_bookmarks_by_stream(self, sync_records):
        """Return the minimum value for the replication key for each stream"""
        min_bookmarks = {}
        for stream, batch in sync_records.items():

            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']
            stream_bookmark_key = self.expected_replication_keys().get(stream, set())
            assert len(stream_bookmark_key) == 1  # There shouldn't be a compound replication key
            (stream_bookmark_key, ) = stream_bookmark_key

            bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]
            min_bookmarks[stream] = {stream_bookmark_key: None}
            for bk_value in bk_values:
                if bk_value is None:
                    continue

                if min_bookmarks[stream][stream_bookmark_key] is None:
                    min_bookmarks[stream][stream_bookmark_key] = bk_value

                if bk_value < min_bookmarks[stream][stream_bookmark_key]:
                    min_bookmarks[stream][stream_bookmark_key] = bk_value
        return min_bookmarks

    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True,
                                      additional_md=[], non_selected_properties=[]):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            if not select_all_fields and not non_selected_properties:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, additional_md, non_selected_properties)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = self.get_properties().get("start_date")
