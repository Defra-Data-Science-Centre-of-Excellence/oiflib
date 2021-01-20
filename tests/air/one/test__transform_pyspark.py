# """Tests for air one processing functions."""

# # Third-party Libraries
# import pytest
# from chispa.dataframe_comparer import assert_df_equality

# # Local libraries
# from oiflib.air.one.transform import (
#     filter_rows,
#     drop_columns,
#     clean_column_values,
#     unpivot,
#     transform_air_one,
# )


# # Define left DataFrame as fixture for use in multiple tests
# @pytest.fixture
# def df_input(spark):
#     """A minimal DataFrame for testing air.one processing functions."""
#     return spark.createDataFrame(
#         data=[
#             ["NH3 Total", 0, 0, 1],
#             ["NOx Total", 0, 0, 2],
#             ["SO2 Total", 0, 0, 3],
#             ["VOC Total", 0, 0, 4],
#             ["PM2.5 Total", 0, 0, 5],
#             ["Another Total", 0, 0, 6],
#         ],
#         schema=[
#             "ShortPollName",
#             "NFRCode",
#             "SourceName",
#             "Value",
#         ],
#     )


# def test_filter_rows(spark, df_input):
#     """Filters the rows as expected."""
#     # Create expected output
#     df_output_expected = spark.createDataFrame(
#         data=[
#             ["NH3 Total", 0, 0, 1],
#             ["NOx Total", 0, 0, 2],
#             ["SO2 Total", 0, 0, 3],
#             ["VOC Total", 0, 0, 4],
#             ["PM2.5 Total", 0, 0, 5],
#         ],
#         schema=[
#             "ShortPollName",
#             "NFRCode",
#             "SourceName",
#             "Value",
#         ],
#     )

#     # Apply function to input
#     df_output_actual = filter_rows(df=df_input)

#     assert_df_equality(df_output_expected, df_output_actual)

#     spark.stop()


# def test_drop_columns(spark, df_input):
#     """Drops the expected columns."""
#     # Create expected output
#     df_output_expected = spark.createDataFrame(
#         data=[
#             ["NH3 Total", 1],
#             ["NOx Total", 2],
#             ["SO2 Total", 3],
#             ["VOC Total", 4],
#             ["PM2.5 Total", 5],
#             ["Another Total", 6],
#         ],
#         schema=[
#             "ShortPollName",
#             "Value",
#         ],
#     )

#     # Apply function to input
#     df_output_actual = drop_columns(df=df_input)

#     assert_df_equality(df_output_expected, df_output_actual)

#     spark.stop()


# def test_clean_column_values(spark, df_input):
#     """Cleans the column values as expected."""
#     # Create expected output
#     df_output_expected = spark.createDataFrame(
#         data=[
#             ["NH3", 0, 0, 1],
#             ["NOx", 0, 0, 2],
#             ["SO2", 0, 0, 3],
#             ["NMVOC", 0, 0, 4],
#             ["PM2.5", 0, 0, 5],
#             ["Another", 0, 0, 6],
#         ],
#         schema=[
#             "ShortPollName",
#             "NFRCode",
#             "SourceName",
#             "Value",
#         ],
#     )

#     # Apply function to input
#     df_output_actual = clean_column_values(df=df_input)

#     assert_df_equality(df_output_expected, df_output_actual)

#     spark.stop()


# def test_unpivot(spark, df_input):
#     """Unpivots the input as expected."""
#     # Create expected output
#     df_output_expected = spark.createDataFrame(
#         data=[
#             ["Another Total", "NFRCode", 0],
#             ["NH3 Total", "NFRCode", 0],
#             ["NOx Total", "NFRCode", 0],
#             ["PM2.5 Total", "NFRCode", 0],
#             ["SO2 Total", "NFRCode", 0],
#             ["VOC Total", "NFRCode", 0],
#             ["Another Total", "SourceName", 0],
#             ["NH3 Total", "SourceName", 0],
#             ["NOx Total", "SourceName", 0],
#             ["PM2.5 Total", "SourceName", 0],
#             ["SO2 Total", "SourceName", 0],
#             ["VOC Total", "SourceName", 0],
#             ["Another Total", "Value", 6],
#             ["NH3 Total", "Value", 1],
#             ["NOx Total", "Value", 2],
#             ["PM2.5 Total", "Value", 5],
#             ["SO2 Total", "Value", 3],
#             ["VOC Total", "Value", 4],
#         ],
#         schema=[
#             "ShortPollName",
#             "Year",
#             "Emissions",
#         ],
#     )

#     # Apply function to input
#     df_output_actual = unpivot(df=df_input)

#     assert_df_equality(df_output_expected, df_output_actual)

#     spark.stop()


# def test_transform_air_one(spark, df_input):
#     """Cleans the column values as expected."""
#     # Create expected output
#     df_output_expected = spark.createDataFrame(
#         data=[
#             ["NH3", "Value", 1],
#             ["NMVOC", "Value", 4],
#             ["NOx", "Value", 2],
#             ["PM2.5", "Value", 5],
#             ["SO2", "Value", 3],
#         ],
#         schema=[
#             "ShortPollName",
#             "Year",
#             "Emissions",
#         ],
#     )

#     # Apply function to input
#     df_output_actual = transform_air_one(df=df_input)

#     assert_df_equality(df_output_expected, df_output_actual)

#     spark.stop()
