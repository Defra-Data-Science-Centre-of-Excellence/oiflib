from pandas import DataFrame, read_excel

from oiflib.core import column_name_to_string
from oiflib.datasets import oif_datasets


def extract(theme: str, number: str) -> DataFrame:
    return read_excel(
        **oif_datasets.get(theme).get(number)
    ).pipe(column_name_to_string)
