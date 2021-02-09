"""Schema for Air Five DataFrames."""
from typing import Dict

from dill import dump  # noqa: S403 - security warnings n/a
from numpy import float64, int64
from pandera import Check, Column, DataFrameSchema

dict_schema: Dict[str, Dict[str, Dict[str, DataFrameSchema]]] = {
    "air": {
        "five": {
            "extracted": DataFrameSchema(
                columns={
                    "Year": Column(
                        pandas_dtype=int64,
                        checks=Check.in_range(1997, 2019),
                        allow_duplicates=False,
                    ),
                    "Site": Column(
                        pandas_dtype=str,
                        checks=Check.equal_to("All sites"),
                    ),
                    "Annual Mean NO2 concentration (µg/m3)": Column(
                        pandas_dtype=float64,
                        checks=Check.in_range(25.0, 65.0),
                    ),
                    "95% confidence interval for 'All sites' (+/-)": Column(
                        pandas_dtype=float64,
                        checks=Check.in_range(0.0, 15.0),
                    ),
                },
                coerce=True,
                strict=True,
            ),
            "enriched": DataFrameSchema(
                columns={
                    "Year": Column(
                        pandas_dtype=int64,
                        checks=Check.in_range(1997, 2019),
                        allow_duplicates=False,
                    ),
                    "Site": Column(
                        pandas_dtype=str,
                        checks=Check.equal_to("All sites"),
                    ),
                    "Annual Mean NO2 concentration (µg/m3)": Column(
                        pandas_dtype=float64,
                        checks=Check.in_range(25.0, 65.0),
                    ),
                    "95% confidence interval for 'All sites' (+/-)": Column(
                        pandas_dtype=float64,
                        checks=Check.in_range(0.0, 15.0),
                    ),
                    "Lower_CI_bound": Column(
                        pandas_dtype=float64,
                        checks=Check.in_range(25.0, 55.0),
                    ),
                    "Upper_CI_bound": Column(
                        pandas_dtype=float64,
                        checks=Check.in_range(30.0, 75.0),
                    ),
                },
                checks=[
                    Check(
                        lambda df: df["Lower_CI_bound"]
                        == (
                            df["Annual Mean NO2 concentration (µg/m3)"]
                            - df["95% confidence interval for 'All sites' (+/-)"]
                        ),
                    ),
                    Check(
                        lambda df: df["Upper_CI_bound"]
                        == (
                            df["Annual Mean NO2 concentration (µg/m3)"]
                            + df["95% confidence interval for 'All sites' (+/-)"]
                        ),
                    ),
                ],
                coerce=True,
                strict=True,
            ),
        },
    },
}

with open("/home/edfawcetttaylor/repos/oiflib/data/schema.pkl", "wb") as file:
    dump(dict_schema, file)
