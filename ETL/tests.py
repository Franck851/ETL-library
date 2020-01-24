# TODO WIP: id_col, row_count
# todo: zone of test: make managed delta tables instead
# def run_tests(config, zones, tables, id_col=None, row_count=None):
#   """Compare one or many dataframes to the truth datasets.
#
#   Select a subset of zones and tables to test/compare with their saved test dataset.
#   Provide a number of rows to reduce testing time in exchange of reliability.
#   (Recommended with complex nested dataframes)
#
#   Parameters
#   ----------
#   config: Config
#     Config instance object
#
#   zones : str or list, optional
#     Zone or list of zones containing the tables to test.
#     Leave empty to use all zones.
#
#   tables: str or list, optional
#     folder name or list of tables names to test.
#     Leave empty to use all tables.
#
#   row_count: int, optional
#     Number of rows to test.
#
#   Raises
#   ------
#   AssertionError
#     When tests fail.
#
#   Examples
#   ________
#   Testing a curated table/dataframe
#
#      1. Read data in raw_test to generate 'source_data_df'
#      2. Run pipeline on 'source_data_df' to generate the 'result_df' df
#      3. Read curated_test to generate the 'expected_df'
#      4. Compare 'result_df' to 'expected_df'
#
#   To control data used to generate a dataframe to test, it is needed to keep a dataset for the source
#   and keep it the test zone.
#   """
#   tables = config.validate_table_names(tables)
#   if row_count is not None and id_col is None:
#     raise ValueError('row_count requires an id_col')
#   if 'raw_test' in zones:
#     raise ValueError('Cannot test raw zone data')
#
#   test_zones = list(filter(lambda zone: zone in config.supported_zones_test))
#
#   for test_zone in test_zones:
#     suite = unittest.TestSuite()
#     source_data_zone = config.get_mount_name_from_zone_name('raw_test' if test_zone == 'curated_test' else 'curated_test')
#     data_zone = config.test_zone_to_data_zone_hash[test_zone]
#
#     for folder in tables:
#       source_data_path = source_data_zone + '/' + config.data_source + '/' + folder
#       source_data_df = spark.read.format('delta').load(source_data_path)
#       assert df_not_empty(source_data_df), 'No source data in ih-{}-zone-tests/{}/{}?'.format(source_data_zone,
#                                                                                               config.data_source,
#                                                                                               folder)
#
#       expected_path = config.get_mount_name_from_zone_name(test_zone) + '/' + config.data_source + '/' + folder
#       expected_df = spark.read.format('delta').load(expected_path)
#       result_df = getattr(data_zone + '_transform_df')(folder, source_data_df)
#
#       if row_count is not None:
#         ids = list(str(id[0]) for id in expected_df.limit(row_count).select(id_col).collect())
#         expected_df = expected_df.filter(col(id_col).isin(ids))
#         result_df = result_df.filter(col(id_col).isin(ids))
#
#       test_name = data_zone + '.' + folder
#       suite.addTest(PySparkTest(expected_df, result_df, test_name))
#
#     unittest.TextTestRunner(verbosity=2).run(suite)
#
#
# def run_custom_test(result_df, expected_df, id_col=None, row_count=None):
#   """WIP Test a custom df/pipeline. Compare 'result_df' to 'expected_df'.
#
#   Provide a number of rows to reduce testing time in exchange of reliability.
#   (Recommended with complex nested dataframes)
#
#   Parameters
#   ----------
#   result_df: pyspark.sql.DataFrame
#     Dataframe to compare with expected_df.
#
#   expected_df: pyspark.sql.DataFrame
#     Source of truth.
#
#   id_col: str, optional
#     Name of the column containing unique values.
#
#   row_count: int, optional
#     Number of rows to test.
#
#   Raises
#   ------
#   AssertionError
#     When tests fail.
#
#   Notes
#   _____
#   If the transformations involved in the process of generating the data introduces randomness
#   leading to an inconsistent number of rows between executions on the same input, provide a
#   column with unique, id-like, values to contrain the test and to only compare rows with those same ids.
#   Tests are run only on rows were the id_col values are intersecting on the result_df and expected_df dataframes.
#   """
#   # TODO WIP
#   if row_count is not None:
#     if id_col is None:
#       raise ValueError('row_count requires an id_col')
#     expected_df = expected_df.limit(row_count)
#     result_df = result_df.limit(row_count)
#
#   suite = unittest.TestSuite()
#
#   if id_col is not None:
#     ids_list = [row[0] for row in expected_df.select(id_col).intersect(result_df.select(id_col)).collect()]
#     log('Will test ' + str(len(ids_list)) + ' rows.')
#     expected_df = expected_df.filter(col(id_col).isin(ids_list))
#     result_df = result_df.filter(col(id_col).isin(ids_list))
#     suite.addTest(PySparkTest(expected_df, result_df, 'Custom test'))
#
#   unittest.TextTestRunner(verbosity=2).run(suite)
