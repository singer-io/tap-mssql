import os
import socket


class TapSpec:
    """ Base class to specify tap-specific configuration. """

    ### TABLE PROPERTIES ###
    DATABASE_NAME = "database-name"
    SCHEMA = "schema-name"
    STREAM = "stream_name"
    VIEW = "is-view"
    PRIMARY_KEYS = "table-key-properties"
    ROWS = "row-count"
    SELECTED = "selected"  # is this for after discovery from field selection?
    FIELDS = "fields"
    VALUES = "values"

    ### FIELD PROPERTIES
    DATATYPE = "sql-datatype"
    INCLUSION = "inclusion"
    DEFAULT_SELECT = "selected-by-default"

    ### OTHERS
    REPLICATION_KEYS = "valid-replication-keys"
    FOREIGN_KEYS = "table-foreign-key-properties"
    AUTOMATIC_FIELDS = "automatic"
    AVAILABLE_FIELDS = "available"
    UNAVAILABLE_FIELDS = "unsupported"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"

    # TODO - This assumes all columns are nullable which isn't true
    DATATYPE_SCHEMAS = {
        "bigint": {"type": ["integer", "null"], "maximum": 2 ** (8 * 8 - 1) - 1, "minimum": -2 ** (8 * 8 - 1)},
        "int":  {"type": ["integer", "null"], "maximum": 2 ** (8 * 4 - 1) - 1, "minimum": -2 ** (8 * 4 - 1)},
        "smallint":  {"type": ["integer", "null"], "maximum": 2 ** (8 * 2 - 1) - 1, "minimum": -2 ** (8 * 2 - 1)},
        "tinyint":  {"type": ["integer", "null"], "maximum": 255, "minimum": 0},
        "bit":  {"type": ["boolean", "null"]},
        "real":  {"type": ["number", "null"]},
        "float":  {"type": ["number", "null"]},
        "numeric": {'multipleOf': 10 ** 0,
                    "type": ["number", "null"],
                    'maximum': 10 ** 18, 'exclusiveMinimum': True,
                    'minimum': -10 ** 18, 'exclusiveMaximum': True},
        "decimal": {'multipleOf': 10 ** 0,
                    "type": ["number", "null"],
                    'maximum': 10 ** 18, 'exclusiveMinimum': True,
                    'minimum': -10 ** 18, 'exclusiveMaximum': True},
        "char": {"type": ["string", "null"], "maxLength": 2, "minLength": 2},  # TODO this is just for char(2)
        "varchar": {"type": ["string", "null"], "maxLength": 8000, "minLength": 0},  # TODO this is just for varchar(800)
        "nvarchar": {"type": ["string", "null"], "maxLength": 8000, "minLength": 0},  # TODO this is just for nvarchar(800)
        "date": {"type": ["string", "null"], 'format': 'date-time'},
        "datetime": {"type": ["string", "null"], 'format': 'date-time'},
        "time": {"type": ["string", "null"]},
    }

    for precision in range(1, 39):
        for scale in range(precision + 1):
            DATATYPE_SCHEMAS["numeric({},{})".format(precision, scale)] = {
                'multipleOf': 10 ** (0 - scale),
                "type": ["number", "null"],
                'maximum':  10 ** (precision - scale), 'exclusiveMinimum': True,
                'minimum': -10 ** (precision - scale), 'exclusiveMaximum': True
            }

    for precision in range(1, 39):
        for scale in range(precision + 1):
            DATATYPE_SCHEMAS["decimal({},{})".format(precision, scale)] = {
                'multipleOf': 10 ** (0 - scale),
                "type": ["number", "null"],
                'maximum':  10 ** (precision - scale), 'exclusiveMinimum': True,
                'minimum': -10 ** (precision - scale), 'exclusiveMaximum': True
            }

    # TODO - BUG  https://stitchdata.atlassian.net/browse/SRCE-1008
    SUPPORTED_DATATYPES = [
        "bigint", "int", "smallint", "tinyint", "bit", "real", "float",
        "date", "datetime", "time", "datetime2", "datetimeoffset", "smalldatetime",
        "char", "varchar", "varchar(max)", "nchar", "nvarchar", "nvarchar(max)",
        "binary", "varbinary", "varbinary(max)", "uniqueidentifier", "timestamp", "rowversion",
        "numeric", "decimal", "money", "smallmoney"
    ]
    SUPPORTED_DATATYPES.extend([
            "numeric({0},{1})".format(precision, scale)
            for precision in range(1, 39)
            for scale in range(precision + 1)
        ])
    SUPPORTED_DATATYPES.extend([
            "decimal({0},{1})".format(precision, scale)
            for precision in range(1, 39)
            for scale in range(precision + 1)
        ])
    SUPPORTED_DATATYPES.extend(["float({0})".format(bits + 1) for bits in range(53)])
    SUPPORTED_DATATYPES.extend(["char({0})".format(chars + 1) for chars in range(8000)])
    SUPPORTED_DATATYPES.extend(["varchar({0})".format(chars + 1) for chars in range(8000)])
    SUPPORTED_DATATYPES.extend(["nchar({0})".format(chars + 1) for chars in range(4000)])
    SUPPORTED_DATATYPES.extend(["nvarchar({0})".format(chars + 1) for chars in range(4000)])
    SUPPORTED_DATATYPES.extend(["binary({0})".format(chars + 1) for chars in range(8000)])
    SUPPORTED_DATATYPES.extend(["varbinary({0})".format(chars + 1) for chars in range(8000)])

    CONFIGURATION_ENVIRONMENT = {
        "properties": {
            "user": "STITCH_TAP_MSSQL_TEST_DATABASE_USER",
            "port": "STITCH_TAP_MSSQL_TEST_DATABASE_PORT"
        },
        "credentials": {
            "password": "STITCH_TAP_MSSQL_TEST_DATABASE_PASSWORD",
        }
    }
    TEST_DB_HOST="localhost"

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "mssql"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.mssql"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        properties_env = self.CONFIGURATION_ENVIRONMENT['properties']
        return_value = {k: os.getenv(v) for k, v in properties_env.items()}
        return_value['host'] = 'localhost'
        return_value['include_schemas_in_destination_stream_name'] = 'true'
        return return_value

    def get_credentials(self):
        """Authentication information for the test account"""
        credentials_env = self.CONFIGURATION_ENVIRONMENT['credentials']
        return {k: os.getenv(v) for k, v in credentials_env.items()}

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        default = {
                self.REPLICATION_KEYS: {"updated_at"},
                self.PRIMARY_KEYS: {"id"},
                self.AVAILABLE_FIELDS: {},  # added, need to add to template
                self.UNAVAILABLE_FIELDS: {},  # added, need to add to template
                self.REPLICATION_METHOD: self.FULL,
                self.API_LIMIT: 250
        }

        meta = default.copy()
        meta.update({self.FOREIGN_KEYS: {"owner_id", "owner_resource"}})

        return {
            "full": default
        }
