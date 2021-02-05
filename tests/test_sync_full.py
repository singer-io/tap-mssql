# """
# Test tap discovery
# """
# import random
# from datetime import date, datetime, timezone, time
# from decimal import getcontext, Decimal
# from json import dumps
# from random import randint, sample
#
# import dateutil.tz
# import sys
#
# from numpy import float32
#
# from tap_tester import menagerie, runner, connections
# 
# from tap_tester.suites.mssql.database import drop_all_user_databases, create_database, \
#     create_table, mssql_cursor_context_manager, insert
#
# from base import BaseTapTest
#
#
# class SyncTesFull(BaseTapTest):
#     """ Test the tap discovery """
#
#     EXPECTED_METADATA = dict()
#
#     def name(self):
#         return "{}_full_sync_test".format(super().name())
#
#     @classmethod
#     def discovery_expected_metadata(cls):
#         """The expected streams and metadata about the streams"""
#
#         return cls.EXPECTED_METADATA
#
#     @classmethod
#     def setUpClass(cls) -> None:
#         """Create the expected schema in the test database"""
#         drop_all_user_databases()
#         database_name = "data_types_database"
#         schema_name = "dbo"
#
#         query_list = list(create_database(database_name, "Latin1_General_CS_AS"))
#         # query_list.extend(create_schema(database_name, schema_name))
#
#         table_name = "integers"
#         column_name = ["pk", "MyBigIntColumn", "MyIntColumn", "MySmallIntColumn"]
#         column_type = ["int", "bigint", "int", "smallint"]
#         # TODO - BUG https://stitchdata.atlassian.net/browse/SRCE-1072
#         primary_key = {"pk"}
#         column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#                                        primary_key=primary_key))
#
#         # create data
#         num_bytes = [8, 4, 2]  # bytes in a bigint, int, smallint and tinyint
#         values = [
#             (0, ) + tuple(-(2 ** (8 * size - 1)) for size in num_bytes),  # min
#             (1, ) + tuple(0 for _ in num_bytes),  # 0
#             (2, ) + tuple(2 ** (8 * size - 1) - 1 for size in num_bytes),  # max
#             (3, None, None, None)  # null
#         ]
#         values.extend([(pk, ) + tuple(randint(-(2 ** (8 * size - 1)), 2 ** (8 * size - 1) - 1)
#                                       for size in num_bytes)
#                        for pk in range(4, 14)])  # random sample values
#         query_list.extend(insert(database_name, schema_name, table_name, values))
#         cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#                                   column_type, primary_key, values)
#
#         table_name = "tiny_integers_and_bools"
#         column_name = ["pk", "MyTinyIntColumn", "my_boolean"]
#         column_type = ["int", "tinyint", "bit"]
#         primary_key = {"pk"}
#         column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#                                        primary_key=primary_key))
#
#         # create data
#         values = [
#             (0, 0, False),  # min
#             (1, 255, True),  # max
#             (2, None, None)  # null
#         ]
#         values.extend([(pk, randint(0, 255), bool(randint(0, 1))) for pk in range(3, 13)])  # random sample values
#         query_list.extend(insert(database_name, schema_name, table_name, values))
#         cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#                                   column_type, primary_key, values)
#
#         # TODO - BUG https://stitchdata.atlassian.net/browse/SRCE-1075
#         table_name = "numeric_precisions"
#         precision_scale = [(precision, randint(0, precision)) for precision in (9, 15)]  # , 19, 28, 38)]
#         column_type = [
#             "numeric({},{})".format(precision, scale)
#             for precision, scale in precision_scale
#         ]
#         column_name = ["pk"] + [x.replace("(", "_").replace(",", "_").replace(")", "") for x in column_type]
#         column_type = ["int"] + column_type
#         primary_key = {"pk"}
#         column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#                                        primary_key=primary_key))
#
#         # generate values for one precision at a time and then zip them together
#         columns = []
#         column = 0
#         for precision, scale in precision_scale:
#             getcontext().prec = precision
#             columns.append([
#                 Decimal(-10 ** precision + 1) / Decimal(10 ** scale),  # min
#                 0,
#                 None,
#                 Decimal(10 ** precision - 1) / Decimal(10 ** scale)  # max
#             ])
#             columns[column].extend([Decimal(random.randint(-10 ** precision + 1, 10 ** precision - 1)) /
#                                    Decimal(10 ** scale)
#                                    for _ in range(10)])
#             column = column + 1
#
#         values = list(zip(range(14), *columns))
#         query_list.extend(insert(database_name, schema_name, table_name, values))
#         cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#                                   column_type, primary_key, values)
#
#         table_name = "decimal_precisions"
#         precision_scale = [(precision, randint(0, precision)) for precision in (9, 15)]  # 19, 28, 38)]
#         column_type = [
#             "decimal({},{})".format(precision, scale)
#             for precision, scale in precision_scale
#         ]
#         column_name = ["pk"] + [x.replace("(", "_").replace(",", "_").replace(")", "") for x in column_type]
#         column_type = ["int"] + column_type
#         primary_key = {"pk"}
#         column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#                                        primary_key=primary_key))
#
#         # generate values for one precision at a time and then zip them together
#         columns = []
#         column = 0
#         for precision, scale in precision_scale:
#             getcontext().prec = precision
#             columns.append([
#                 Decimal(-10 ** precision + 1) / Decimal(10 ** scale),  # min
#                 0,
#                 None,
#                 Decimal(10 ** precision - 1) / Decimal(10 ** scale)  # max
#             ])
#             columns[column].extend([Decimal(random.randint(-10 ** precision + 1, 10 ** precision - 1)) /
#                                     Decimal(10 ** scale)
#                                     for _ in range(10)])
#             column = column + 1
#
#         values = list(zip(range(14), *columns))
#         query_list.extend(insert(database_name, schema_name, table_name, values))
#         cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#                                   column_type, primary_key, values)
#
#         # TODO - BUG https://stitchdata.atlassian.net/browse/SRCE-1078
#         table_name = "float_precisions"
#         column_name = ["pk", "float_24", "float_53", "real_24_bits"]
#         column_type = ["int", "float(24)", "float(53)", "real"]
#         primary_key = {"pk"}
#         column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#                                        primary_key=primary_key))
#
#         # create data
#         values = [
#             (0,
#              float(str(float32(1.175494351e-38))),
#              2.2250738585072014e-308,
#              float(str(float32(1.175494351e-38)))),  # min positive
#             (1, float(str(float32(3.402823466e+38))), 1.7976931348623158e+308, float(str(float32(3.402823466e+38)))),  # max positive
#             (2, float(str(float32(-1.175494351e-38))), -2.2250738585072014e-308, float(str(float32(-1.175494351e-38)))),  # smallest negative
#             (3, float(str(float32(-3.402823466e+38))), -1.7976931348623158e+308, float(str(float32(-3.402823466e+38)))),  # largest negative
#             (4, 0.0, 0.0, 0.0),  # 0
#             (5, None, None, None),  # Null
#             # (float("Inf"), -float("Inf"), float('NaN'))
#         ]
#
#         # # random small positive values
#         # values.extend([(
#         #     pk,
#         #     random.uniform(1.175494351e-38, 10 ** random.randint(-37, 0)),
#         #     random.uniform(2.2250738585072014e-308, 10 ** random.randint(-307, 0)),
#         #     random.uniform(1.175494351e-38, 10 ** random.randint(-37, 0))
#         #     ) for pk in range(6, 11)
#         # ])
#         #
#         # # random large positive values
#         # values.extend([(
#         #     pk,
#         #     random.uniform(1, 10 ** random.randint(0, 38)),
#         #     random.uniform(1, 10 ** random.randint(0, 308)),
#         #     random.uniform(1, 10 ** random.randint(0, 38))
#         #     ) for pk in range(11, 16)
#         # ])
#         #
#         # # random small negative values
#         # values.extend([(
#         #     pk,
#         #     random.uniform(-1.175494351e-38, -1 * 10 ** random.randint(-37, 0)),
#         #     random.uniform(-2.2250738585072014e-308, -1 * 10 ** random.randint(-307, 0)),
#         #     random.uniform(-1.175494351e-38, -1 * 10 ** random.randint(-37, 0))
#         #     ) for pk in range(16, 21)
#         # ])
#         #
#         # # random large negative values
#         # values.extend([(
#         #     pk,
#         #     random.uniform(-1,  -1 *10 ** random.randint(0, 38)),
#         #     random.uniform(-1,  -1 *10 ** random.randint(0, 308)),
#         #     random.uniform(-1,  -1 *10 ** random.randint(0, 38))
#         #     ) for pk in range(21, 26)
#         # ])
#         query_list.extend(insert(database_name, schema_name, table_name, values))
#         cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#                                   column_type, primary_key, values)
#
#         # # TODO - BUG https://stitchdata.atlassian.net/browse/SRCE-1080
#         # table_name = "dates_and_times"
#         # column_name = ["just_a_date", "date_and_time", "bigger_range_and_precision_datetime",
#         #                "datetime_with_timezones", "datetime_no_seconds", "its_time"]
#         # column_type = ["date", "datetime", "datetime2", "datetimeoffset", "smalldatetime", "time"]
#         #
#         # # TODO - remove this once more datetime types are supported. Also, things shouldn't blow up
#         # #   when they aren't supported
#         # column_name = ["pk", "just_a_date", "date_and_time", "its_time"]
#         # column_type = ["int", "date", "datetime", "time"]
#         # primary_key = set()
#         # column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         # query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#         #                                primary_key=primary_key))
#         #
#         # # create data
#         # values = [
#         #     (
#         #         0,
#         #         date(1, 1, 1),
#         #         datetime(1753, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc),
#         #         # datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc),
#         #         # datetime.fromtimestamp(
#         #         #     datetime(1, 1, 1, 14, 0, 0, 0, tzinfo=timezone.utc).timestamp(),
#         #         #     tz=dateutil.tz.tzoffset(None, -14*60)),
#         #         # datetime(1900, 1, 1, 0, 0, tzinfo=timezone.utc),
#         #         time(0, 0, 0, 0, tzinfo=timezone.utc),
#         #     ),  # min
#         #     (
#         #         1,
#         #         date(9999, 12, 31),
#         #         datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
#         #         # datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc),
#         #         # datetime.fromtimestamp(
#         #         #     datetime(9999, 12, 31, 9, 59, 59, 999999, tzinfo=timezone.utc).timestamp(),
#         #         #     tz=dateutil.tz.tzoffset(None, 14*60)),
#         #         # datetime(2079, 6, 6, 23, 59, tzinfo=timezone.utc),
#         #         time(23, 59, 59, 999999, tzinfo=timezone.utc),
#         #     ),  # max
#         #     (2, None, None, None)  # , None, None, None),  # Null
#         # ]
#         #
#         # # TODO - make some random samples...
#         #
#         # query_list.extend(insert(database_name, schema_name, table_name, values))
#         # cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#         #                           column_type, primary_key, values)
#
#         table_name = "char_data"
#         column_name = ["pk", "char_2"]  # , "char_8000"]
#         column_type = ["int", "char(2)"]  # , "char(8000)"]
#         primary_key = {"pk"}
#         column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#                                        primary_key=primary_key))
#
#         # use all valid unicode characters
#         chars = list(range(0, 55296))
#         chars.extend(range(57344, sys.maxunicode))
#         chars.reverse()
#
#         values = [(pk, "".join([chr(chars.pop()) for _ in range(2)])) for pk in range(16)]
#         cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#                                   column_type, primary_key, values)
#         query_list.extend(insert(database_name, schema_name, table_name, values))
#
#         table_name = "varchar_data"
#         column_name = ["pk", "varchar_5", "varchar_8000", "varchar_max"]
#         column_type = ["int", "varchar(5)", "varchar(8000)", "varchar(max)"]
#         primary_key = {"pk"}
#         column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#                                        primary_key=primary_key))
#
#         values = [
#             (pk,
#              chr(chars.pop()),
#              "".join([chr(chars.pop()) for _ in range(15)]),
#              "".join([chr(chars.pop()) for _ in range(randint(1, 16))])
#              ) for pk in range(3)
#         ]
#         values.extend([(50, None, None, None), ])
#         query_list.extend(insert(database_name, schema_name, table_name, values))
#         cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#                                   column_type, primary_key, values)
#
#         # table_name = "nchar_data"
#         # column_name = ["pk", "nchar_8"]  # , "nchar_4000"]
#         # column_type = ["int", "nchar(8)"]  # , "nchar(4000)"]
#         # primary_key = {"pk"}
#         # column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         # query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#         #                                primary_key=primary_key))
#         # values = [
#         #     (pk,
#         #      "".join([chr(chars.pop()) for _ in range(4)])) #chr(chars.pop()))  # , "".join([chr(chars.pop()) for _ in range(1500)]))
#         #     for pk in range(1)
#         # ]
#         # # values.extend([(50, None, None), ])
#         # query_list.extend(insert(database_name, schema_name, table_name, values))
#         # cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#         #                           column_type, primary_key, values)
#         #
#         table_name = "nvarchar_data"
#         column_name = ["pk", "nvarchar_5", "nvarchar_4000", "nvarchar_max"]
#         column_type = ["int", "nvarchar(5)", "nvarchar(4000)", "nvarchar(max)"]
#         primary_key = {"pk"}
#         column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#                                        primary_key=primary_key))
#         chars.reverse()
#         values = [
#             (pk,
#              chr(chars.pop()),
#              "".join([chr(chars.pop()) for _ in range(8)]),
#              "".join([chr(chars.pop()) for _ in range(randint(1, 8))])
#              ) for pk in range(1)
#         ]
#         values.extend([(50, None, None, None), ])
#
#         # pk = 51
#         # while len(chars):
#         #     #  Use the rest of the characters
#         #     values.extend([(
#         #         pk,
#         #         chr(chars.pop()),
#         #         "".join([chr(chars.pop()) for _ in range(min(len(chars), 800))]) if len(chars) else "",
#         #         "".join([chr(chars.pop()) for _ in range(min(len(chars), randint(1, 800)))]) if len(chars) else "",
#         #         "".join([chr(chars.pop()) for _ in range(min(len(chars), randint(1, random_types[0] // 10)))]),
#         #         "".join([chr(chars.pop()) for _ in range(min(len(chars), randint(1, random_types[1] // 10)))]),
#         #         "".join([chr(chars.pop()) for _ in range(min(len(chars), randint(1, random_types[2] // 10)))])
#         #     )])
#         #     pk += 1
#
#         query_list.extend(insert(database_name, schema_name, table_name, values))
#         cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#                                   column_type, primary_key, values)
#
#         # query_list.extend(['-- there are {} characters left to test'.format(len(chars))])
#         #
#         #
#         table_name = "money_money_money"
#         column_name = ["pk", "cash_money", "change"]
#         column_type = ["int", "money", "smallmoney"]
#         primary_key = {"pk"}
#         column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#                                        primary_key=primary_key))
#         values = [
#             (0, 123.45, 0.99),
#             (1, 123213.99, 0.00)
#         ]
#         query_list.extend(insert(database_name, schema_name, table_name, values))
#         column_name = ["pk"]
#         column_type = ["int"]
#         values = [
#             (0, ),
#             (1, )
#         ]
#         cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#                                   column_type, primary_key, values)
#
#         # table_name = "binary_data"
#         # column_name = ["binary_1", "binary_8000"]
#         # column_type = ["binary(1)", "binary(8000)"]
#         # primary_key = set()
#         # column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         # query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#         #                                primary_key=primary_key))
#         # cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#         #                           column_type, primary_key)
#         #
#         # table_name = "varbinary_data"
#         # column_name = ["varbinary_1", "varbinary_8000", "varbinary_max"]
#         # column_type = ["varbinary(1)", "varbinary(8000)", "varbinary(max)"]
#         # random_types = [x for x in sample(range(1, 8000), 3)]
#         # column_name.extend(["varbinary_{0}".format(x) for x in random_types])
#         # column_type.extend(["varbinary({0})".format(x) for x in random_types])
#         # primary_key = set()
#         # column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         # query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#         #                                primary_key=primary_key))
#         # cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#         #                           column_type, primary_key)
#         #
#         # table_name = "text_and_image_deprecated_soon"
#         # column_name = ["nvarchar_text", "varchar_text", "varbinary_data",
#         #                "rowversion_synonym_timestamp"]
#         # column_type = ["ntext", "text", "image", "timestamp"]
#         # primary_key = set()
#         # column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         # query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#         #                                primary_key=primary_key))
#         # cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#         #                           column_type, primary_key)
#         #
#         # table_name = "weirdos"
#         # column_name = [
#         #     "geospacial", "geospacial_map", "markup", "guid", "version", "tree",
#         #     "variant", "SpecialPurposeColumns"
#         # ]
#         # column_type = [
#         #     "geometry", "geography", "xml", "uniqueidentifier", "rowversion", "hierarchyid",
#         #     "sql_variant", "xml COLUMN_SET FOR ALL_SPARSE_COLUMNS"
#         # ]
#         # primary_key = set()
#         # column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         # query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#         #                                primary_key=primary_key))
#         # column_type[7] = "xml"  # this is the underlying type
#         # cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#         #                           column_type, primary_key)
#         #
#         # table_name = "computed_columns"
#         # column_name = ["started_at", "ended_at", "durations_days"]
#         # column_type = ["datetimeoffset", "datetimeoffset", "AS DATEDIFF(day, started_at, ended_at)"]
#         # primary_key = set()
#         # column_def = [" ".join(x) for x in list(zip(column_name, column_type))]
#         # query_list.extend(create_table(database_name, schema_name, table_name, column_def,
#         #                                primary_key=primary_key))
#         # column_type[2] = "int"  # this is the underlying type of a datediff
#         # cls.add_expected_metadata(cls, database_name, schema_name, table_name, column_name,
#         #                           column_type, primary_key)
#
#         mssql_cursor_context_manager(*query_list)
#
#         cls.expected_metadata = cls.discovery_expected_metadata
#
#     def test_run(self):
#         """
#         Verify that a full sync can send capture all data and send it in the correct format
#         """
#
#         # run in check mode
#         check_job_name = runner.run_check_mode(self, conn_id)
#
#         # verify check  exit codes
#         exit_status = menagerie.get_exit_status(conn_id, check_job_name)
#         menagerie.verify_check_exit_status(self, exit_status, check_job_name)
#
#         # get the catalog information of discovery
#         found_catalogs = menagerie.get_catalogs(conn_id)
#         found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
#
#         # verify that persisted streams have the correct properties
#         test_catalog = found_catalogs[0]
#
#         additional_md = [{"breadcrumb": [], "metadata": {'replication-method': 'FULL_TABLE'}}]
#         BaseTapTest.select_all_streams_and_fields(
#             conn_id, found_catalogs, additional_md=additional_md,
#             non_selected_properties=["cash_money", "change"])
#
#         # clear state
#         menagerie.set_state(conn_id, {})
#         sync_job_name = runner.run_sync_mode(self, conn_id)
#
#         # verify tap and target exit codes
#         exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
#         menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
#
#         # verify record counts of streams
#         record_count_by_stream = runner.examine_target_output_file(
#             self, conn_id, self.expected_streams(), self.expected_primary_keys_by_stream_id())
#         expected_count = {k: len(v['values']) for k, v in self.expected_metadata().items()}
#         self.assertEqual(record_count_by_stream, expected_count)
#
#         # verify records match on the first sync
#         records_by_stream = runner.get_records_from_target_output()
#
#         for stream in self.expected_streams():
#             with self.subTest(stream=stream):
#                 stream_expected_data = self.expected_metadata()[stream]
#                 # TODO - test schema matches expectations based on data type, nullable, not nullable, datetimes as string +, etc
#                 #   This needs to be consistent based on replication method so you can change replication methods
#                 table_version = records_by_stream[stream]['table_version']
#
#                 # verify on the first sync you get activate version message before and after all data
#                 self.assertEqual(
#                     records_by_stream[stream]['messages'][0]['action'],
#                     'activate_version')
#                 self.assertEqual(
#                     records_by_stream[stream]['messages'][-1]['action'],
#                     'activate_version')
#                 column_names = [
#                     list(field_data.keys())[0] for field_data in stream_expected_data[self.FIELDS]
#                 ]
#
#                 expected_messages = [
#                     {
#                         "action": "upsert", "data":
#                         {
#                             column: value for column, value
#                             in list(zip(column_names, stream_expected_data[self.VALUES][row]))
#                         }
#                     } for row in range(len(stream_expected_data[self.VALUES]))
#                 ]
#
#                 # remove sequences from actual values for comparison
#                 [message.pop("sequence") for message
#                  in records_by_stream[stream]['messages'][1:-1]]
#
#                 # Verify all data is correct
#                 for expected_row, actual_row in list(
#                         zip(expected_messages, records_by_stream[stream]['messages'][1:-1])):
#                     with self.subTest(expected_row=expected_row):
#                         self.assertEqual(actual_row["action"], "upsert")
#                         self.assertEqual(len(expected_row["data"].keys()), len(actual_row["data"].keys()),
#                                          msg="there are not the same number of columns")
#
#                         for column_name, expected_value in expected_row["data"].items():
#                             column_index = [list(key.keys())[0] for key in
#                                             self.expected_metadata()[stream][self.FIELDS]].index(column_name)
#
#                             if isinstance(expected_value, Decimal):
#                                 self.assertEqual(type(actual_row["data"][column_name]), float,
#                                                  msg="decimal value is not represented as a number")
#                                 self.assertEqual(expected_value, Decimal(str(actual_row["data"][column_name])),
#                                                  msg="expected: {} != actual {}".format(
#                                                      expected_row, actual_row))
#                             elif self.expected_metadata()[stream][self.FIELDS][column_index
#                                     ][column_name][self.DATATYPE] == "real":
#                                 if actual_row["data"][column_name] is None:
#                                     self.assertEqual(expected_value, actual_row["data"][column_name],
#                                                      msg="expected: {} != actual {}".format(
#                                                          expected_row, actual_row))
#                                 else:
#                                     self.assertEqual(type(actual_row["data"][column_name]), float,
#                                                      msg="float value is not represented as a number")
#                                     self.assertEqual(float(str(float32(expected_value))),
#                                                      float(str(float32(actual_row["data"][column_name]))),
#                                                      msg="single value of {} doesn't match actual {}".format(
#                                                          float(str(float32(expected_value))),
#                                                          float(str(float32(actual_row["data"][column_name]))))
#                                                      )
#                             else:
#                                 self.assertEqual(expected_value, actual_row["data"][column_name],
#                                                  msg="expected: {} != actual {}".format(
#                                                      expected_row, actual_row))
#                 print("records are correct for stream {}".format(stream))
#
#                 # verify state and bookmarks
#                 state = menagerie.get_state(conn_id)
#
#                 bookmark = state['bookmarks'][stream]
#
#                 self.assertIsNone(state.get('currently_syncing'), msg="expected state's currently_syncing to be None")
#                 # TODO - change this to something for mssql once binlog (cdc) is finalized and we know what it is
#                 self.assertIsNone(
#                     bookmark.get('lsn'),
#                     msg="expected bookmark for stream to have NO lsn because we are using full-table replication")
#
#                 self.assertEqual(bookmark['version'], table_version,
#                                  msg="expected bookmark for stream to match version")
#
#                 expected_schemas = {
#                     "selected": True,
#                     "type": "object",
#                     "properties": {
#                         k: dict(
#                             **self.DATATYPE_SCHEMAS[v["sql-datatype"]],
#                             selected=True,
#                             inclusion=v["inclusion"]
#                         )
#                         for fd in stream_expected_data[self.FIELDS] for k, v in fd.items()
#                     }
#                 }
#
#                 # I made everything nullable except pks.  Based on this the DATATYPE_SCHEMAS reflects nullable types
#                 # we need to update the pk to be not nullable
#                 expected_schemas["properties"]["pk"]["type"] = ["integer"]
#                 self.assertEqual(records_by_stream[stream]['schema'],
#                                  expected_schemas,
#                                  msg="expected: {} != actual: {}".format(expected_schemas,
#                                                                          records_by_stream[stream]['schema']))
#
#         # ----------------------------------------------------------------------
#         # invoke the sync job AGAIN and get the same 3 records
#         # ----------------------------------------------------------------------
#         # TODO - update the table to add a column and ensure that discovery adds the new column
#         sync_job_name = runner.run_sync_mode(self, conn_id)
#
#         # verify tap and target exit codes
#         exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
#         menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
#         record_count_by_stream = runner.examine_target_output_file(
#             self, conn_id, self.expected_streams(), self.expected_primary_keys_by_stream_id())
#         expected_count = {k: len(v['values']) for k, v in self.expected_metadata().items()}
#         self.assertEqual(record_count_by_stream, expected_count)
#         records_by_stream = runner.get_records_from_target_output()
#
#         for stream in self.expected_streams():
#             with self.subTest(stream=stream):
#                 stream_expected_data = self.expected_metadata()[stream]
#                 # TODO - test schema matches expectations based on data type, nullable, not nullable, datetimes as string +, etc
#                 #   This needs to be consistent based on replication method so you can change replication methods
#                 # {'action': 'upsert', 'sequence': 1560362044666000001, 'data': {'MySmallIntColumn': 0, 'pk': 1, 'MyIntColumn': 0, 'MyBigIntColumn': 0}}
#
#                 new_table_version = records_by_stream[stream]['table_version']
#
#                 # verify on a subsequent sync you get activate version message only after all data
#                 self.assertEqual(
#                     records_by_stream[stream]['messages'][0]['action'],
#                     'upsert')
#                 self.assertEqual(
#                     records_by_stream[stream]['messages'][-1]['action'],
#                     'activate_version')
#                 column_names = [
#                     list(field_data.keys())[0] for field_data in stream_expected_data[self.FIELDS]
#                 ]
#
#                 expected_messages = [
#                     {
#                         "action": "upsert", "data":
#                         {
#                             column: value for column, value
#                             in list(zip(column_names, stream_expected_data[self.VALUES][row]))
#                         }
#                     } for row in range(len(stream_expected_data[self.VALUES]))
#                 ]
#
#                 # remove sequences from actual values for comparison
#                 [message.pop("sequence") for message
#                  in records_by_stream[stream]['messages'][0:-1]]
#
#                 # Verify all data is correct
#                 for expected_row, actual_row in list(
#                         zip(expected_messages, records_by_stream[stream]['messages'][0:-1])):
#                     with self.subTest(expected_row=expected_row):
#                         self.assertEqual(actual_row["action"], "upsert")
#                         self.assertEqual(len(expected_row["data"].keys()), len(actual_row["data"].keys()),
#                                          msg="there are not the same number of columns")
#
#                         for column_name, expected_value in expected_row["data"].items():
#                             column_index = [list(key.keys())[0] for key in
#                                             self.expected_metadata()[stream][self.FIELDS]].index(column_name)
#
#                             if isinstance(expected_value, Decimal):
#                                 self.assertEqual(type(actual_row["data"][column_name]), float,
#                                                  msg="decimal value is not represented as a number")
#                                 self.assertEqual(expected_value, Decimal(str(actual_row["data"][column_name])),
#                                                  msg="expected: {} != actual {}".format(
#                                                      expected_row, actual_row))
#                             elif self.expected_metadata()[stream][self.FIELDS][column_index
#                             ][column_name][self.DATATYPE] == "real":
#                                 if actual_row["data"][column_name] is None:
#                                     self.assertEqual(expected_value, actual_row["data"][column_name],
#                                                      msg="expected: {} != actual {}".format(
#                                                          expected_row, actual_row))
#                                 else:
#                                     self.assertEqual(type(actual_row["data"][column_name]), float,
#                                                      msg="float value is not represented as a number")
#                                     self.assertEqual(float(str(float32(expected_value))),
#                                                      float(str(float32(actual_row["data"][column_name]))),
#                                                      msg="single value of {} doesn't match actual {}".format(
#                                                          float(str(float32(expected_value))),
#                                                          float(str(float32(actual_row["data"][column_name]))))
#                                                      )
#                             else:
#                                 self.assertEqual(expected_value, actual_row["data"][column_name],
#                                                  msg="expected: {} != actual {}".format(
#                                                      expected_row, actual_row))
#                 print("records are correct for stream {}".format(stream))
#
#                 # verify state and bookmarks
#                 state = menagerie.get_state(conn_id)
#
#                 bookmark = state['bookmarks'][stream]
#                 self.assertIsNone(state.get('currently_syncing'), msg="expected state's currently_syncing to be None")
#
#                 self.assertIsNone(
#                     bookmark.get('lsn'),
#                     msg="expected bookmark for stream to have NO lsn because we are using full-table replication")
#                 self.assertGreater(new_table_version, table_version,
#                                    msg="table version {} didn't increate from {} on the second run".format(
#                                        new_table_version,
#                                        table_version))
#                 self.assertEqual(bookmark['version'], new_table_version,
#                                  msg="expected bookmark for stream to match version")
#
#                 expected_schemas = {
#                     "selected": True,
#                     "type": "object",
#                     "properties": {
#                         k: dict(
#                             **self.DATATYPE_SCHEMAS[v["sql-datatype"]],
#                             selected=True,
#                             inclusion=v["inclusion"]
#                         )
#                         for fd in stream_expected_data[self.FIELDS] for k, v in fd.items()
#                     }
#                 }
#
#                 # I made everything nullable except pks.  Based on this the DATATYPE_SCHEMAS reflects nullable types
#                 # we need to update the pk to be not nullable
#                 expected_schemas["properties"]["pk"]["type"] = ["integer"]
#                 self.assertEqual(records_by_stream[stream]['schema'],
#                                  expected_schemas,
#                                  msg="expected: {} != actual: {}".format(expected_schemas,
#                                                                          records_by_stream[stream]['schema']))
#
#
# # SCENARIOS.add(SyncTesFull) THIS TEST WAS RE-WRITTEN IN SMALLER PARTS
