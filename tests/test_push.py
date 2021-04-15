"""Tests for push module."""
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from typing import Optional, Tuple, Union

from _pytest._code.code import ExceptionInfo
from git import Repo
from git.objects.commit import Commit
from pandas import read_csv
from pandas.core.frame import DataFrame
from pandas.testing import assert_frame_equal
from pytest import raises
from pytest_cases import fixture_ref, parametrize

from oiflib.push import (
    _add,
    _commit,
    _push,
    _reset_local_branch,
    _set_data_file_name,
    _write_to_csv,
    publish,
)


def test__reset_local_branch(
    local_git_repo: Tuple[str, str, str, Repo],
) -> None:
    """Local branch is reset."""
    _root_name: str = local_git_repo[0]
    _repo_name: str = local_git_repo[1]
    _folder_name: str = local_git_repo[2]
    _test_file_name: str = "test_file.txt"

    open(
        f"{_root_name}/{_repo_name}/{_folder_name}/{_test_file_name}",
        "wb",
    ).close()

    _repo: Repo = local_git_repo[3]

    _repo.index.add(f"{_folder_name}/{_test_file_name}")

    _commit(
        root=_root_name,
        repo=_repo_name,
        indicator_code="test_indicator",
        data_commit_message="test commit message",
    )

    _repo.remotes.origin.fetch()

    remote: Commit = _repo.commit("origin")

    local_before_reset: Commit = _repo.commit("HEAD")

    _reset_local_branch(
        root=_root_name,
        repo=_repo_name,
        remote="origin",
    )

    local_after_reset: Commit = _repo.commit("HEAD")

    assert local_before_reset != remote
    assert local_after_reset == remote


@parametrize(
    "indicator_code, expected",
    [
        ("A1", "indicator_1-1-1.csv"),
        ("B1b", "indicator_2-1-2.csv"),
        ("C2ai", "indicator_3-2-1-a.csv"),
        ("D7i", "indicator_4-7-1-a.csv"),
        ("Z99", "indicator_0-0-0.csv"),
    ],
    ids=[
        "for two-part OIF code",
        "for three-part OIF code",
        "for four-part OIF code",
        "for four-part OIF code with missing third part",
        "for code not in lookup",
    ],
)
def test__set_data_file_name(indicator_code, expected) -> None:
    """A filename is returned."""
    returned: str = _set_data_file_name(
        indicator_code=indicator_code,
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


@parametrize(
    "local_git_repo, expectation",
    [
        (fixture_ref("local_git_repo"), does_not_raise()),
        (
            ("invalid_root", "invalid_repo", "invalid_folder_name", Repo()),
            raises(Exception),
        ),
    ],
    ids=[
        "with valid local repo",
        "with invalid local repo",
    ],
)
def test__add(
    local_git_repo: Tuple[str, str, str, Repo],
    expectation: ExceptionInfo,
) -> None:
    """File is added to git index."""
    _root_name: str = local_git_repo[0]
    _repo_name: str = local_git_repo[1]
    _folder_name: str = local_git_repo[2]
    _test_file_name: str = "test_file.txt"

    with expectation:

        open(
            f"{_root_name}/{_repo_name}/{_folder_name}/{_test_file_name}", "wb"
        ).close()

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
    "local_git_repo, data_commit_message, expected, expectation",
    [
        (
            fixture_ref("local_git_repo"),
            None,
            "add data for test_indicator",
            does_not_raise(),
        ),
        (
            fixture_ref("local_git_repo"),
            "initial commit",
            "initial commit",
            does_not_raise(),
        ),
        (
            ("invalid_root", "invalid_repo", "invalid_folder_name", Repo()),
            "initial commit",
            "initial commit",
            raises(Exception),
        ),
    ],
    ids=[
        "with generated commit message",
        "with provided commit message",
        "with invalid local repo",
    ],
)
def test__commit(
    local_git_repo: Tuple[str, str, str, Repo],
    data_commit_message: Optional[str],
    expected: str,
    expectation: ExceptionInfo,
) -> None:
    """File is committed with generated or provided message."""
    _root_name: str = local_git_repo[0]
    _repo_name: str = local_git_repo[1]
    _folder_name: str = local_git_repo[2]
    _test_file_name: str = "test_file.txt"

    with expectation:

        open(
            f"{_root_name}/{_repo_name}/{_folder_name}/{_test_file_name}", "wb"
        ).close()

        _repo: Repo = local_git_repo[3]

        _repo.index.add(f"{_folder_name}/{_test_file_name}")


        _commit(
            root=_root_name,
            repo=_repo_name,
            indicator_code="test_indicator",
            data_commit_message=data_commit_message,
        )

        _most_recent_commit_message: str = _repo.head.commit.message

        assert _most_recent_commit_message == expected


@parametrize(
    "branches, local_git_repo, expectation",
    [
        ("test", fixture_ref("local_git_repo"), does_not_raise()),
        (("test", "new"), fixture_ref("local_git_repo"), does_not_raise()),
        (
            ("test", "new"),
            ("invalid_root", "invalid_repo", "invalid_folder_name", Repo()),
            raises(Exception),
        ),
    ],
    ids=[
        "to same branch",
        "to different branch",
        "with invalid local repo",
    ],
)
def test__push(
    branches: Union[str, Tuple[str, str]],
    local_git_repo: Tuple[str, str, str, Repo],
    expectation: ExceptionInfo,
) -> None:
    """File is pushed to remote."""
    _root_name: str = local_git_repo[0]
    _repo_name: str = local_git_repo[1]
    _folder_name: str = local_git_repo[2]
    _remote: str = "origin"
    _test_file_name: str = "test_file.txt"

    with expectation:

        open(
            f"{_root_name}/{_repo_name}/{_folder_name}/{_test_file_name}",
            "wb",
        ).close()

        _repo: Repo = local_git_repo[3]

        _repo.index.add(f"{_folder_name}/{_test_file_name}")
        _repo.index.commit("test commit")

        _push(
            root=_root_name,
            repo=_repo_name,
            remote=_remote,
            branches=branches,
        )

        _repo.remotes.origin.fetch()

        local: Commit = _repo.commit("HEAD")

        remote: Commit

        if isinstance(branches, tuple):
            remote = _repo.commit(f"origin/{branches[1]}")
        else:
            remote = _repo.commit(f"origin/{branches}")

        assert local == remote


def test_publish(
    expected_air_one_formatted_rename_disaggregation_column: DataFrame,
    local_git_repo: Tuple[str, str, str, Repo],
) -> None:
    """DataFrame is pushed to remote."""  # noqa: D403 - capitalisation is correct
    _branches: str = "test"

    publish(
        df=expected_air_one_formatted_rename_disaggregation_column,
        indicator_code="test indicator",
        branches=_branches,
        root=local_git_repo[0],
        repo=local_git_repo[1],
        data_folder=local_git_repo[2],
    )

    _repo: Repo = local_git_repo[3]

    _repo.remotes.origin.fetch()

    local: Commit = _repo.commit("HEAD")

    remote: Commit = _repo.commit(f"origin/{_branches}")

    assert local == remote
