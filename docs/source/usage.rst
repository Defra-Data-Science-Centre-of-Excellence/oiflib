Usage
=====

The package provides generic **extract** and **validate** functions and theme/indicator
specific **transform** and **enrich** functions.

Extract
-------

The generic :ref:`extract_function` function extracts a DataFrame for a given **theme**
and **indicator** from an Excel or OpenDocument workbook. It uses the **theme** and
**indicator** values to look up where to read the workbook from in datasets.json_
dictionary.

.. _datasets.json: https://github.com/Defra-Data-Science-Centre-of-Excellence/OIF-Pipeline-Logic/blob/EFT-Defra/issue33/data/datasets.json

The dictionary contains key-value pairs of parameters and arguments that are passed to
pandas.read_excel_ method.

.. _pandas.read_excel: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html

For example, the dictionary contains the following pairs for Air One::

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

- **io** is the path to the workbook. This can be a URL (as above) or a local file path.
- **sheet_name** is the name of the sheet you want to extract.
- **usecols** is the range of columns to extract.
- **skiprows** is the number of rows (starting at 0) to skip before the header row of the
  table you want to extract.
- **nrows** is total number of rows to read, excluding the header row.

To import the extract function run:

>>> from oiflib.extract import extract

To use it to extract the Air One DataFrame run:

>>> extract(theme="air", indicator="one")
