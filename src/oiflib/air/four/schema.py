"""Schema for Air Four DataFrames."""

from pandera import Check, Column, DataFrameSchema, Float, Int, String

schema_extracted: DataFrameSchema = DataFrameSchema(
    columns={
        "Year": Column(
            pandas_dtype=Int,
            checks=Check.in_range(1987, 2019),
            allow_duplicates=False,
        ),
        "Site": Column(
            pandas_dtype=String,
            checks=Check.equal_to("All sites"),
        ),
        "Annual average maximum daily 8-hour mean O3 concentration (µg/m3)": Column(
            pandas_dtype=Float,
            checks=Check.in_range(50.0, 80.0),
        ),
        "95% confidence interval for 'All sites' (+/-)": Column(
            pandas_dtype=Float,
            checks=Check.in_range(0.0, 20.0),
        ),
    }
)

schema_enriched: DataFrameSchema = DataFrameSchema(
    columns={
        "Year": Column(
            pandas_dtype=Int,
            checks=Check.in_range(1987, 2019),
            allow_duplicates=False,
        ),
        "Site": Column(
            pandas_dtype=String,
            checks=Check.equal_to("All sites"),
        ),
        "Annual average maximum daily 8-hour mean O3 concentration (µg/m3)": Column(
            pandas_dtype=Float,
            checks=Check.in_range(50.0, 80.0),
        ),
        "95% confidence interval for 'All sites' (+/-)": Column(
            pandas_dtype=Float,
            checks=Check.in_range(0.0, 20.0),
        ),
        "Lower_CI_bound": Column(
            pandas_dtype=Float,
            checks=Check.in_range(30.0, 80.0),
        ),
        "Upper_CI_bound": Column(
            pandas_dtype=Float,
            checks=Check.in_range(60.0, 80.0),
        ),
    },
    checks=[
        Check(
            lambda df: df["Lower_CI_bound"]
            == (
                df["Annual average maximum daily 8-hour mean O3 concentration (µg/m3)"]
                - df["95% confidence interval for 'All sites' (+/-)"]
            ),
        ),
        Check(
            lambda df: df["Upper_CI_bound"]
            == (
                df["Annual average maximum daily 8-hour mean O3 concentration (µg/m3)"]
                + df["95% confidence interval for 'All sites' (+/-)"]
            ),
        ),
    ],
)
