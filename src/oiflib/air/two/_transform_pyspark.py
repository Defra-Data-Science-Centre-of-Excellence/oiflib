"""TODO module docstring."""

# import pandas as pd
# import sys
# from pyspark import RDD
# from pyspark.sql import SparkSession, Window, Row
# from pyspark.sql.functions import lit, last
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType


# spark = SparkSession.builder.master("local").appName("oiflib").getOrCreate()

# schema = StructType(
#     [
#         StructField("NCFormat", StringType(), True),
#         StructField("IPCC", StringType(), True),
#         StructField("BaseYear", StringType(), True),
#         StructField("1990", DoubleType(), True),
#         StructField("1995", DoubleType(), True),
#         StructField("1998", DoubleType(), True),
#         StructField("1999", DoubleType(), True),
#         StructField("2000", DoubleType(), True),
#         StructField("2001", DoubleType(), True),
#         StructField("2002", DoubleType(), True),
#         StructField("2003", DoubleType(), True),
#         StructField("2004", DoubleType(), True),
#         StructField("2005", DoubleType(), True),
#         StructField("2006", DoubleType(), True),
#         StructField("2007", DoubleType(), True),
#         StructField("2008", DoubleType(), True),
#         StructField("2009", DoubleType(), True),
#         StructField("2010", DoubleType(), True),
#         StructField("2011", DoubleType(), True),
#         StructField("2012", DoubleType(), True),
#         StructField("2013", DoubleType(), True),
#         StructField("2014", DoubleType(), True),
#         StructField("2015", DoubleType(), True),
#         StructField("2016", DoubleType(), True),
#         StructField("2017", DoubleType(), True),
#         StructField("2018", DoubleType(), True),
#     ]
# )

# data = pd.read_excel(
#     io="https://uk-air.defra.gov.uk/assets/documents/reports/cat09/2006160834_DA_GHGI_1990-2018_v01-04.xlsm",  # noqa: B950
#     sheet_name="England By Source",
#     usecols="B:AA",
#     skiprows=16,
# )

# df_raw = spark.createDataFrame(
#     data=data,
#     schema=schema,
# )


# df_raw.NCFormat = df_raw.NCFormat.ffill()


# def pyspark_ffill(rdd: RDD, key: str):
#     value: str = ""
#     if key in rdd:
#         value: str = rdd[key] if rdd[key] != "NaN"
#         rdd[key]: str = rdd[key] if rdd[key] != "NaN" else value
#     return rdd


# df_raw.rdd.map(lambda x: pyspark_ffill(x, "NCFormat")).toDF().show(10)


# for x in df_raw.select("NCFormat").collect():
#     value: Row = x if x["NCFormat"] != "NaN" else value
#     value


# df_raw["category"] = None

# df_raw.loc[
#     df_raw.NCFormat.str.contains("Land use") & df_raw.IPCC.str.contains("4[A|G]"),
#     "category",
# ] = "Forestry sink"
# df_raw.loc[df_raw.NCFormat == "Agriculture", "category"] = "Agriculture"
# df_raw.loc[
#     df_raw.NCFormat.str.contains("Business")
#     & df_raw.IPCC.str.contains("2[E-F]|2G[1-2]"),
#     "category",
# ] = "Fluorinated gases"
# df_raw.loc[
#     df_raw.NCFormat.str.contains("Industrial")
#     & df_raw.IPCC.str.contains("2B9|2C[3-4]"),
#     "category",
# ] = "Fluorinated gases"
# df_raw.loc[
#     df_raw.NCFormat.str.contains("Residential") & df_raw.IPCC.str.contains("2F4"),
#     "category",
# ] = "Fluorinated gases"
# df_raw.loc[
#     df_raw.NCFormat.str.contains("Land use")
#     & df_raw.IPCC.str.contains("4[B-E|_]"),
#     "category",
# ] = "Land use & land use change"
# df_raw.loc[
#     df_raw.NCFormat.str.contains("Business") & df_raw.IPCC.str.contains("5C"),
#     "category",
# ] = "Waste"
# df_raw.loc[
#     df_raw.NCFormat.str.contains("Residential")
#     & df_raw.IPCC.str.contains("5[B-C]"),
#     "category",
# ] = "Waste"
# df_raw.loc[
#     df_raw.NCFormat.str.contains("Waste") & df_raw.IPCC.str.contains("5[A-D]"),
#     "category",
# ] = "Waste"

# df_a2_melted = (
#     df_raw.groupby("category")
#     .sum()
#     .reset_index()
#     .melt(id_vars="category", var_name="Year", value_name="GHG - kt CO2 Equiv")
# )
