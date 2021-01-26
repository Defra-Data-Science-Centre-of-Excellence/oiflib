"""."""

from typing import Dict, Union

from pandas import DataFrame, read_excel

from oiflib.core import column_name_to_string
from oiflib.datasets import oif_datasets


def extract(theme: str, dataset: str) -> DataFrame:
    """Reads in data for the theme and dataset specified.

    This function extracts a DataFrame from an Excel or OpenDocument workbook based on
    the metadata provided by the oif_datasets metadata dictionary. First, it searches
    for the given "theme" within oif_datasets. If it finds this, it searches for the
    "dataset" within theme metadata dictionary. If it finds this, it unpacks the keys
    and values and uses them as paramaters and arguments for read_excel(). Finally, it
    converts the column names of the extracted DataFrame to string using
    column_name_to_string(). This convertion is necessary for subsequent validation.

    Args:
        theme (str): Theme name, as a lower case string. E.g. "air".
        dataset (str): Dataset number, as a lower case string. E.g. "one".

    Returns:
        DataFrame: The DataFrame for the given theme and dataset.
    """
    metadata: Dict[str, Dict[str, Dict[str, Union[str, int]]]] = oif_datasets
    if metadata is not None:
        try:
            theme_metadata: Dict[str, Dict[str, Union[str, int]]] = metadata[theme]
        except KeyError:
            print(f"Theme: {theme} does not exist.")
        else:
            if theme_metadata is not None:
                try:
                    dataset_metadata: Dict[str, Union[str, int]] = theme_metadata[
                        dataset
                    ]
                except KeyError:
                    print(f"Dataset: { dataset } does not exist in Theme: { theme }.")
                else:
                    if dataset_metadata is not None:
                        return read_excel(**dataset_metadata).pipe(
                            column_name_to_string
                        )
