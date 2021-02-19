Usage
=====

The package provides generic Extract_ and Validate_ functions and theme and indicator
specific Transform_ and Enrich_ functions.

Extract
-------

The generic :ref:`extract_function` function extracts a DataFrame for a given **theme**
and **indicator** from an Excel or OpenDocument workbook. It uses the **theme** and
**indicator** values to look up the location of the workbook in a JSON format metadata
dictionary_.

.. _dictionary: https://github.com/Defra-Data-Science-Centre-of-Excellence/OIF-Pipeline-Logic/blob/EFT-Defra/issue33/data/datasets.json

The dictionary contains key-value pairs of parameters and arguments that are passed to
pandas.read_excel_ method.

.. _pandas.read_excel: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html

For example, the dictionary contains the following information for Air One::

    {
        "air": {
            "one": {
                "io": "http://uk-air.defra.gov.uk/reports/cat09/2010220959_DA_API_1990-2018_V1.0.xlsx",
                "sheet_name": "England API",
                "usecols": "B:AA",
                "skiprows": 13,
                "nrows": 1602
            },
    ...
        }
    }

Where:

- **io** is the path to the workbook you want to extract a DataFrame from. This can be a
  URL (as above) or a local file path.
- **sheet_name** is the name of the sheet containing the DataFrame you want to extract.
- **usecols** is the column range the DataFrame you want to extract.
- **skiprows** is the number of rows (starting at 0) to skip before the header row of the
  DataFrame you want to extract.
- **nrows** is total number of rows to read, excluding the header row.

The above is therefore the equivalent of the range ``'England API'!$B$14:$AA$1616`` in Excel
notation.

To import the extract function run:

>>> from oiflib.core import extract

To use it to extract the Air One DataFrame, pass it the relevant **theme** and
**indicator** arguments as lower-case strings:

>>> extracted = extract(theme="air", indicator="one")

Transform
---------

The library provides theme and indicator specific functions to convert an extracted
DataFrame into a tidy format by performing a series for pre-defined transformations,
such as selecting columns, filtering rows, and/or unpivoting wide-format DataFrames
into long-format DataFrames.

For example, the Air One :ref:`air_one_transform_module` module defines functions to
filter out unused rows, drop unused columns, clean values, and unpivot the data. These
functions can be called individually but they are intended to be called by a wrapper
function ``transform_air_one`` that calls them in a pre-defined order. The use of a
wrapper function should improve the readability of high-level code and allow future
developers to add additional transformations.

To import the ``transform_air_one`` function run:

>>> from oiflib.air.one import transform_air_one

To use it to transform the extracted Air One DataFrame run:

>>> transformed = transform_air_one(df=extracted)

Enrich
------

Similarly, the library provides theme and indicator specific functions to enrich a
transformed DataFrame by adding derived columns.

For example, the Air One :ref:`air_one_enrich_module` module defines a function to
index emissions to base year. Again this function could be called explicitly but it
is intended to be called via the wrapper ``enrich_air_one``.

To import the ``enrich_air_one`` function run:

>>> from oiflib.air.one import enrich_air_one

To use it to enrich the transformed Air One DataFrame run:

>>> enriched = enrich_air_one(df=transformed)

Validate
--------

The library uses the pandera_ package for schema-based data validation.

.. _pandera: https://pandera.readthedocs.io/

It provides a generic :ref:`validate_function` function that validates a given DataFrame against a
schema for a given **theme**, **indicator**, and processing **stage**

The schema contains information about a DataFrame's columns. It define the column names,
data-types, and allow you to check the values against various constraints.

For example, the schema of the extracted Air One DataFrame contains the following information:

::

    {
        "air": {
                "one": {
                        "extracted": DataFrameSchema(
                            columns={
                                    "ShortPollName": Column(
                                        pandas_dtype=str,
                                        checks=Check.isin(
                                            [
                                                "B[a]p",
                                                "B[a]p Total",
                                                "CO",
                                                ...
                                                "SO2 Total",
                                                "VOC",
                                                "VOC Total",
                                            ],
                                        ),
                                    ),
                                    "NFRCode": Column(
                                        pandas_dtype=str,
                                        checks=Check.isin(
                                            [
                                                "1A1a",
                                                "1A1b",
                                                "1A1c",
                                                ...
                                                "5D2",
                                                "5E",
                                                "6A",
                                            ],
                                        ),
                                        nullable=True,
                                    ),
                                    "SourceName": Column(
                                        pandas_dtype=str,
                                        checks=Check.isin(
                                            [
                                                "Accidental fires - dwellings",
                                                "Accidental fires - other buildings",
                                                "Accidental fires - vehicles",
                                                ...
                                                "Yarding",
                                                "Zinc alloy and semis production",
                                                "Zinc oxide production",
                                            ],
                                        ),
                                        nullable=True,
                                    ),
                                    r"\d{4}": Column(
                                        pandas_dtype=float,
                                        nullable=True,
                                        regex=True
                                    ),
                            },
                            coerce=True,
                            strict=True,
                        ),
                ...
                }
        ...
        }
    ...
    }

- This schema checks whether the columns ``ShortPollName``, ``NFRCode``, ``SourceName``, and any
  number of column names consisting of four digits exist.
- It checks that the first three contain string values, while the others contain float
  values.
- It checks that the first three contain values from pre-defined lists.
- It checks that ``ShortPollName`` doesn't contain any null values.

Schemas such as this power the validation function. If the DataFrame passed to the
validation function conforms to the schema, it is returned, if not the validation
function raises an error. This allows you insert validation functions calls between
each processing stage.

To import the ``validate`` function run:

>>> from oiflib.core import validate

To use it to validate the extracted Air One DataFrame run:

>>> extracted_validated = validate(theme="air", indicator="one", stage="extracted", df=extracted)

The Full Workflow
-----------------

>>> # import oiflib functions
>>> from oiflib.core import extract, validate
>>> from oiflib.air.one import enrich_air_one, transform_air_one
>>>
>>> # extract and validate air one dataframe
>>> extracted = extract(theme="air", indicator="one")
>>> extracted_validated = validate(theme="air", indicator="one", stage="extracted", df=extracted)
>>>
>>> # transform and validate
>>> transformed = transform_air_one(extracted_validated)
>>> transformed_validated = validate(theme="air", indicator="one", stage="transformed", df=transformed)
>>>
>>> # enrich and validate
>>> enriched = enrich_air_one(transformed_validated)
>>> enriched_validated = validate(theme="air", indicator="one", stage="enriched", df=enriched)
