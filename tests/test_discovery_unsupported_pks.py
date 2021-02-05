"""
Test tap discovery
"""

from tap_tester import menagerie

from database import drop_all_user_databases, create_database, \
    create_table, mssql_cursor_context_manager

from base import BaseTapTest


class DiscoveryTestUnsupportedKeys(BaseTapTest):
    """ Test the tap discovery """

    EXPECTED_METADATA = dict()

    def name(self):
        return "{}_discovery_test_unsupported_pks".format(super().name())

    @classmethod
    def discovery_expected_metadata(cls):
        """The expected streams and metadata about the streams"""

        return cls.EXPECTED_METADATA

    @classmethod
    def setUpClass(cls) -> None:
        """Create the expected schema in the test database"""
        # drop_all_user_databases()
        # database_name = "unsupported_pk_database"
        # schema_name = "dbo"
        #
        # query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
        # # query_list.extend(create_schema(database_name, schema_name))
        #
        # table_name = "unsupported_pk"
        # column_name = ["column_name"]
        # column_type = ["datetimeoffset"]
        # primary_key = {"column_name"}
        # column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
        # query_list.extend(create_table(database_name, schema_name, table_name, column_def,
        #                                primary_key=primary_key))
        # cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
        #                           column_type, primary_key)
        #
        # mssql_cursor_context_manager(*query_list)
        #
        # cls.expected_metadata = cls.discovery_expected_metadata

    def test_run(self):
        """
        Default Test Setup
        Remove previous connections (with the same name)
        Create a new connection (with the properties and credentials above)
        Run discovery and ensure it completes successfully
        """
        print("running test {}".format(self.name()))
        self.create_connection()
