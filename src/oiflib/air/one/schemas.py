"""TODO module docstring."""

from pandera import Check, Column, DataFrameSchema, Float, Int, String

schema_extracted: DataFrameSchema = DataFrameSchema(
    columns={
        "ShortPollName": Column(String),
        "NFRCode": Column(String, nullable=True),
        "SourceName": Column(String, nullable=True),
        r"\d{4}": Column(Float, nullable=True, regex=True),
    },
    coerce=True,
)

schema_transformed: DataFrameSchema = DataFrameSchema(
    columns={
        "ShortPollName": Column(
            String, checks=Check.isin(["NH3", "NOx", "SO2", "NMVOC", "PM2.5"])
        ),
        "Year": Column(Int, checks=Check.greater_than_or_equal_to(1990)),
        "Emissions": Column(Float, checks=Check.greater_than_or_equal_to(0.0)),
    },
    coerce=True,
)

schema_enriched: DataFrameSchema = schema_transformed.add_columns(
    extra_schema_cols={
        "Index": Column(Float, checks=Check.greater_than_or_equal_to(0.0)),
    },
)
