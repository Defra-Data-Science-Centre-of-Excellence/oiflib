"""Tests for push module."""
from pathlib import Path
from typing import Optional, Tuple, Union

from git import Repo
from pandas import read_csv
from pandas.core.frame import DataFrame
from pandas.testing import assert_frame_equal
from pytest_cases import parametrize

from oiflib.push import (
    _add,
    _commit,
    _push,
    _set_data_file_name,
    _write_to_csv,
    publish,
)


@parametrize(
    "theme, indicator, expected",
    [
        ("air", "one", "indicator_1-1-1.csv"),
        ("international", "four", "indicator_0-0-4.csv"),
    ],
    ids=[
        "for air one",
        "for international four",
    ],
)
def test__set_data_file_name(theme, indicator, expected) -> None:
    """A filename is returned."""
    returned: str = _set_data_file_name(
        theme=theme,
        indicator=indicator,
    )

    assert returned == expected


def test__write_to_csv(
    tmp_path: Path,
    expected_air_one_formatted_rename_disaggregation_column: DataFrame,
) -> None:
    """A DataFrame is written to csv."""
    test_repo: Path = tmp_path / "test-repo"

    test_repo.mkdir()

    test_folder: Path = test_repo / "test"

    test_folder.mkdir()

    file_path: str = _write_to_csv(
        df=expected_air_one_formatted_rename_disaggregation_column,
        root=str(tmp_path),
        repo="test-repo",
        data_folder="test",
        data_file_name="test.csv",
    )

    written: DataFrame = read_csv(
        filepath_or_buffer=file_path,
    )

    assert_frame_equal(
        left=written,
        right=expected_air_one_formatted_rename_disaggregation_column,
    )


def test__add(local_git_repo: Tuple[str, str, str, Repo]) -> None:
    """File is added to git index."""
    _root_name: str = local_git_repo[0]
    _repo_name: str = local_git_repo[1]
    _folder_name: str = local_git_repo[2]
    _test_file_name: str = "test_file.txt"

    open(f"{_root_name}/{_repo_name}/{_folder_name}/{_test_file_name}", "wb").close()

    _add(
        root=_root_name,
        repo=_repo_name,
        data_folder=_folder_name,
        data_file_name=_test_file_name,
    )

    _repo: Repo = local_git_repo[3]

    _most_recently_added_item: str = [
        index_item.a_path for index_item in _repo.index.diff("HEAD")
    ].pop()

    assert _most_recently_added_item == f"{_folder_name}/{_test_file_name}"


@parametrize(
    "data_commit_message, expected",
    [
        (None, "add data for test_theme test_indicator"),
        ("initial commit", "initial commit"),
    ],
    ids=[
        "with generated commit message",
        "with provided commit message",
    ],
)
def test__commit(
    local_git_repo: Tuple[str, str, str, Repo],
    data_commit_message: Optional[str],
    expected: str,
) -> None:
    """File is committed with generated or provided message."""
    _root_name: str = local_git_repo[0]
    _repo_name: str = local_git_repo[1]
    _folder_name: str = local_git_repo[2]
    _test_file_name: str = "test_file.txt"

    open(f"{_root_name}/{_repo_name}/{_folder_name}/{_test_file_name}", "wb").close()

    _repo: Repo = local_git_repo[3]

    _repo.index.add(f"{_folder_name}/{_test_file_name}")

    _commit(
        root=_root_name,
        repo=_repo_name,
        theme="test_theme",
        indicator="test_indicator",
        data_commit_message=data_commit_message,
    )

    _most_recent_commit_message: str = _repo.head.commit.message

    assert _most_recent_commit_message == expected


@parametrize(
    "branches",
    [
        ("test"),
        (("test", "master")),
    ],
    ids=[
        "to same branch",
        "to different branch",
    ],
)
def test__push(
    branches: Union[str, Tuple[str, str]], local_git_repo: Tuple[str, str, str, Repo]
) -> None:
    """File is pushed to remote."""
    _root_name: str = local_git_repo[0]
    _repo_name: str = local_git_repo[1]
    _folder_name: str = local_git_repo[2]
    _test_file_name: str = "test_file.txt"

    open(f"{_root_name}/{_repo_name}/{_folder_name}/{_test_file_name}", "wb").close()

    _repo: Repo = local_git_repo[3]

    _repo.index.add(f"{_folder_name}/{_test_file_name}")
    _repo.index.commit("test commit")

    _push(
        root=_root_name,
        repo=_repo_name,
        branches=branches,
    )

    _repo.remotes.origin.fetch()

    commits_behind = _repo.iter_commits("master..origin/master")

    commits_ahead = _repo.iter_commits("origin/master..master")

    count = sum(1 for commits in commits_ahead) + sum(1 for commits in commits_behind)

    assert count == 0


def test_publish(
    expected_air_one_formatted_rename_disaggregation_column: DataFrame,
    local_git_repo: Tuple[str, str, str, Repo],
) -> None:
    """DataFrame is pushed to remote."""  # noqa: D403 - capitalisation is correct
    publish(
        df=expected_air_one_formatted_rename_disaggregation_column,
        theme="air",
        indicator="one",
        branches="test",
        root=local_git_repo[0],
        repo=local_git_repo[1],
        data_folder=local_git_repo[2],
    )

    _repo: Repo = local_git_repo[3]

    _repo.remotes.origin.fetch()

    commits_behind = _repo.iter_commits("master..origin/master")

    commits_ahead = _repo.iter_commits("origin/master..master")

    count = sum(1 for commits in commits_ahead) + sum(1 for commits in commits_behind)

    assert count == 0
