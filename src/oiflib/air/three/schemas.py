"""Schemas for Air Three DataFrames."""

from typing import List

from pandera import Check, Column, DataFrameSchema, Float, Int, String

values_Area_code: List[str] = [
    "Eng",
    "Wal",
    "Sco",
    "Nir",
]

values_Country: List[str] = [
    "England",
    "Wales",
    "Scotland",
    "Northern Ireland",
]

values_measure: List[str] = [
    "total",
    "non-anthropogenic",
    "anthropogenic",
]

schema_extracted: DataFrameSchema = DataFrameSchema(
    columns={
        "Area code": Column(
            pandas_dtype=String,
            checks=Check.isin(values_Area_code),
            allow_duplicates=False,
        ),
        "Country": Column(
            pandas_dtype=String,
            checks=Check.isin(values_Country),
            allow_duplicates=False,
        ),
        r"PM2.5 201[0-9] \(total\)": Column(
            pandas_dtype=Float,
            checks=Check.in_range(float(0), float(15)),
            regex=True,
        ),
        r"PM2.5 201[0-9] \(non-anthropogenic\)": Column(
            pandas_dtype=Float,
            checks=Check.in_range(float(0), float(5)),
            regex=True,
        ),
        r"PM2.5 201[0-9] \(anthropogenic\)": Column(
            pandas_dtype=Float,
            checks=Check.in_range(float(0), float(10)),
            regex=True,
        ),
    },
    coerce=True,
    strict=True,
)

schema_transformed: DataFrameSchema = DataFrameSchema(
    columns={
        "Area code": Column(
            pandas_dtype=String,
            checks=Check.isin(values_Area_code),
        ),
        "Country": Column(
            pandas_dtype=String,
            checks=Check.isin(values_Country),
        ),
        "year": Column(
            pandas_dtype=Int,
            checks=Check.in_range(2011, 2019),
        ),
        "measure": Column(
            pandas_dtype=String,
            checks=Check.isin(values_measure),
        ),
        "ugm-3": Column(
            pandas_dtype=Float,
            checks=Check.in_range(float(0), float(15)),
        ),
    },
    coerce=True,
    strict=True,
    ordered=True,
)
