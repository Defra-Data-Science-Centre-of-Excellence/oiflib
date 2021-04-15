"""Test configuration file for package-level modules."""
# Standard library imports
from json import dump as dump_json
from os import environ
from pathlib import Path
from typing import Any, Dict, Tuple

# Third party imports
from _pytest.tmpdir import TempPathFactory
from boto3 import resource
from dill import dump as dump_pickle  # noqa: S403 - security warnings n/a
from git import Repo
from moto import mock_s3
from pandas import DataFrame
from pandera import Check, Column, DataFrameSchema
from pytest import fixture


# Input and output objects needed to multiple tests
@fixture(scope="module")
def df_extracted_input() -> DataFrame:
    """A minimal input DataFrame for testing the extract module."""
    return DataFrame(
        data={
            1: ["a", "b"],
            2: [4, 5],
            3: [6.7, 8.9],
        },
    )


@fixture(scope="module")
def df_extracted_output() -> DataFrame:
    """A minimal output DataFrame for testing the extract module."""
    return DataFrame(
        data={
            "1": ["a", "b"],
            "2": [4, 5],
            "3": [6.7, 8.9],
        },
    )


@fixture(scope="module")
def file_xlsx(tmp_path_factory: TempPathFactory, df_extracted_input: DataFrame) -> str:
    """Writes a DataFrame to a temporary Excel file, returns the path as a string."""
    path: Path = tmp_path_factory.getbasetemp() / "test.xlsx"

    path_as_string = str(path)

    df_extracted_input.to_excel(
        excel_writer=path_as_string,
        sheet_name="Sheet",
        startrow=5,
        startcol=5,
        index=False,
        float_format="%.2f",
    )

    return path_as_string


@fixture(scope="module")
def file_csv(tmp_path_factory: TempPathFactory, df_extracted_input: DataFrame) -> str:
    """Writes a DataFrame to a temporary CSV file, returns the path as a string."""
    path: Path = tmp_path_factory.getbasetemp() / "test.csv"

    path_as_string = str(path)

    df_extracted_input.to_csv(
        path_or_buf=path_as_string,
        float_format="%.2f",
        index=False,
    )

    return path_as_string


@fixture(scope="module")
def file_pkl(
    tmp_path_factory: TempPathFactory,
    schema_dict: Dict[str, Dict[str, Dict[str, DataFrameSchema]]],
) -> str:
    """Dill pickles dict, writes to temp file, returns path as string."""
    path: Path = tmp_path_factory.getbasetemp() / "test.pkl"

    path_as_string: str = str(path)

    with open(path_as_string, "wb") as file:
        dump_pickle(schema_dict, file)

    return path_as_string


@fixture(scope="module")
def file_json(
    tmp_path_factory: TempPathFactory,
    dict_theme: Dict[str, Dict[str, Dict[str, Any]]],
) -> str:
    """Converts dict to JSON object, writes to temp file, returns path as string."""
    path: Path = tmp_path_factory.getbasetemp() / "test.json"

    path_as_string: str = str(path)

    with open(path, "w") as file:
        dump_json(dict_theme, file)

    return path_as_string


@fixture(scope="module")
def kwargs_xlsx(file_xlsx: str) -> Dict[str, Any]:
    """Returns a dictionary of key word arguments to be passed to read_excel()."""
    return {
        "io": file_xlsx,
        "sheet_name": "Sheet",
        "usecols": [5, 6, 7],
        "skiprows": 5,
        "nrows": 2,
    }


@fixture(scope="module")
def kwargs_csv(file_csv: str) -> Dict[str, Any]:
    """Returns a dictionary of key word arguments to be passed to read_excel()."""
    return {
        "filepath_or_buffer": file_csv,
    }


@fixture(scope="module")
def dict_theme(
    kwargs_xlsx: Dict[str, Any], kwargs_csv: Dict[str, Any]
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Example dictionary."""
    return {
        "theme": {
            "indicator_xlsx": kwargs_xlsx,
            "indicator_csv": kwargs_csv,
        }
    }


@fixture(scope="module")
def test_aws_credentials():
    """Mocked AWS Credentials for moto."""
    environ["AWS_ACCESS_KEY_ID"] = "testing"
    environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # noqa: S105 - fake creds
    environ["AWS_SECURITY_TOKEN"] = "testing"  # noqa: S105 - fake creds
    environ["AWS_SESSION_TOKEN"] = "testing"  # noqa: S105 - fake creds


@fixture(scope="module")
def test_s3_resource(test_aws_credentials):
    """Mock s3 resource for testing."""
    with mock_s3():
        yield resource("s3", region_name="us-east-1")


@fixture(scope="module")
def schema() -> DataFrameSchema:
    """A minimal schema for testing the validate module."""
    return DataFrameSchema(
        columns={
            "1": Column(
                pandas_dtype=str,
                checks=Check.isin(["a", "b"]),
                allow_duplicates=False,
            ),
            "2": Column(
                pandas_dtype=int,
                checks=Check.in_range(4, 5),
                allow_duplicates=False,
            ),
            "3": Column(
                pandas_dtype=float,
                checks=Check.in_range(6.0, 9.0),
                allow_duplicates=False,
            ),
        },
        coerce=True,
        strict=True,
    )


@fixture(scope="module")
def schema_dict(
    schema: DataFrameSchema,
) -> Dict[str, Dict[str, Dict[str, DataFrameSchema]]]:
    """A minimal schema dictionary for testing the validate module."""
    return {
        "test_theme": {
            "test_indicator": {
                "test_stage": schema,
            },
        },
    }


@fixture
def expected_air_one_enriched() -> DataFrame:
    """Example of enriched Air One DataFrame."""
    return DataFrame(
        data={
            "ShortPollName": ["NH3", "NOx", "SO2", "NMVOC", "PM2.5"] * 2,
            "EmissionYear": [1990] * 5 + [1991] * 5,
            "Emission": [2] * 5 + [3] * 5,
            "Index": [2 / 2 * 100] * 5 + [3 / 2 * 100] * 5,
        },
    )


@fixture
def kwargs_no_disaggregation_column() -> Dict[str, str]:
    """Additional kwargs when there's no disaggregation column."""
    return {}


@fixture
def kwargs_disaggregation_column() -> Dict[str, str]:
    """Additional kwargs when the disaggregation column doesn't need to be renamed."""
    return {
        "disaggregation_column": "ShortPollName",
    }


@fixture
def kwargs_rename_disaggregation_column() -> Dict[str, str]:
    """Additional kwargs when the disaggregation column does need to be renamed."""
    return {
        "disaggregation_column": "ShortPollName",
        "disaggregation_column_new": "Pollutant",
    }


@fixture
def expected_air_one_formatted_no_disaggregation_column() -> DataFrame:
    """Example of formatted Air One DataFrame."""
    return DataFrame(
        data={
            "Year": [1990] * 5 + [1991] * 5,
            "Value": [2 / 2 * 100] * 5 + [3 / 2 * 100] * 5,
        },
    )


@fixture
def expected_air_one_formatted_disaggregation_column() -> DataFrame:
    """Example of formatted Air One DataFrame."""
    return DataFrame(
        data={
            "Year": [1990] * 5 + [1991] * 5,
            "ShortPollName": ["NH3", "NOx", "SO2", "NMVOC", "PM2.5"] * 2,
            "Value": [2 / 2 * 100] * 5 + [3 / 2 * 100] * 5,
        },
    )


@fixture
def expected_air_one_formatted_rename_disaggregation_column() -> DataFrame:
    """Example of formatted Air One DataFrame."""
    return DataFrame(
        data={
            "Year": [1990] * 5 + [1991] * 5,
            "Pollutant": ["NH3", "NOx", "SO2", "NMVOC", "PM2.5"] * 2,
            "Value": [2 / 2 * 100] * 5 + [3 / 2 * 100] * 5,
        },
    )


@fixture
def remote_git_repo(tmp_path: Path) -> Tuple[str, str, str, Repo]:
    """Remote git repo for testing."""
    _root_name: str = str(tmp_path)

    _repo_name: str = "test_remote_repo"

    _repo_path: Path = tmp_path / _repo_name

    _repo_path.mkdir()

    _repo = Repo.init(f"{_root_name}/{_repo_name}")

    _folder_name: str = "test_folder"

    _folder_path: Path = _repo_path / _folder_name

    _folder_path.mkdir()

    _init_file_name: str = "init_file.txt"

    _init_file_path: Path = _folder_path / _init_file_name

    open(_init_file_path, "wb").close()

    _repo.index.add([str(_init_file_path)])

    _repo.index.commit("initial commit")

    _repo.create_head("new")

    return (_root_name, _repo_name, _folder_name, _repo)


@fixture
def local_git_repo(
    tmp_path: Path, remote_git_repo: Tuple[str, str, str, Repo]
) -> Tuple[str, str, str, Repo]:
    """Local git repo for testing."""
    _root_name: str = str(tmp_path)

    _repo_name: str = "test_local_repo"

    _repo_path: Path = tmp_path / _repo_name

    _repo_path.mkdir()

    _repo = Repo.clone_from(
        f"{_root_name}/{remote_git_repo[1]}",
        str(_repo_path),
    )

    _repo.create_head("test")

    _repo.heads.test.checkout()

    _folder_name: str = remote_git_repo[2]

    return (_root_name, _repo_name, _folder_name, _repo)
