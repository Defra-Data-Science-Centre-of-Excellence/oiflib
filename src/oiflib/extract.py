"""."""

from json import load
from typing import Dict, Union

from pandas import DataFrame, read_excel

from oiflib.core import column_name_to_string


def _dict_from_json_file(path: str) -> Dict[str, Dict[str, Dict[str, Union[str, int]]]]:
    """Read OIF dataset dictionary from JSON file.

    Args:
        path (str): path to JSON file containing OIF datasets dictionary.

    Returns:
        Dict[str, Dict[str, Dict[str, Union[str, int]]]]: Python dict of OIF datasets.
    """
    with open(file=path, mode="r") as file_json:
        dictionary: Dict[str, Dict[str, Dict[str, Union[str, int]]]] = load(file_json)
    return dictionary


def extract(
    theme: str,
    indicator: str,
    path: str = "/home/edfawcetttaylor/repos/oiflib/data/datasets.json",
) -> DataFrame:
    """Reads in data for the theme and indicator specified.

    This function extracts a DataFrame from an Excel or OpenDocument workbook based on
    the metadata provided by the oif_datasets metadata dictionary. First, it searches
    for the given "theme" within oif_datasets. If it finds this, it searches for the
    "indicator" within theme metadata dictionary. If it finds this, it unpacks the keys
    and values and uses them as paramaters and arguments for read_excel(). Finally, it
    converts the column names of the extracted DataFrame to string using
    column_name_to_string(). This convertion is necessary for subsequent validation.

    Args:
        path (str): path to JSON file containing OIF datasets dictionary. Defaults to
            "/home/edfawcetttaylor/repos/oiflib/data/datasets.json".
        theme (str): Theme name, as a lower case string. E.g. "air".
        indicator (str): Indicator number, as a lower case string. E.g. "one".

    Returns:
        DataFrame: The DataFrame for the given theme and indicator.
    """
    metadata: Dict[str, Dict[str, Dict[str, Union[str, int]]]] = _dict_from_json_file(
        path,
    )

    if metadata is not None:
        try:
            theme_metadata: Dict[str, Dict[str, Union[str, int]]] = metadata[theme]
        except KeyError:
            print(f"Theme: {theme} does not exist.")
        else:
            if theme_metadata is not None:
                try:
                    dataset_metadata: Dict[str, Union[str, int]] = theme_metadata[
                        indicator
                    ]
                except KeyError:
                    print(
                        f"Indicator: { indicator } does not exist in Theme: { theme }."
                    )
                else:
                    if dataset_metadata is not None:
                        return read_excel(**dataset_metadata).pipe(
                            column_name_to_string
                        )
