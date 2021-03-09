"""Tests for the validate module."""
from typing import Dict, Optional

from dill import dumps  # noqa: S403 - security warnings n/a
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pandera import DataFrameSchema

from oiflib.validate import (
    _dict_from_pickle,
    _dict_from_pickle_local,
    _dict_from_pickle_s3,
    _schema_from_dict,
    validate,
)


def test__dict_from_pickle_local(
    file_pkl: str,
    schema_dict: Dict[str, Dict[str, Dict[str, DataFrameSchema]]],
) -> None:
    """Placeholder."""
    _ = _dict_from_pickle_local(
        file_path=file_pkl,
    )
    assert _ == schema_dict


def test__dict_from_pickle_s3(
    test_s3_resource,
    schema_dict: Dict[str, Dict[str, Dict[str, DataFrameSchema]]],
    bucket_name: str = "s3-ranch-019",
    object_key: str = "schemas.pkl",
) -> None:
    """Returns same dictionary."""
    test_s3_resource.create_bucket(Bucket=bucket_name)

    test_s3_object = test_s3_resource.Object(
        bucket_name=bucket_name,
        key=object_key,
    )

    pickled_obj: bytes = dumps(schema_dict)

    test_s3_object.put(
        Body=pickled_obj,
    )

    returned = _dict_from_pickle_s3(
        bucket_name=bucket_name,
        object_key=object_key,
    )

    assert returned == schema_dict


class TestDictFromPickle(object):
    """Tests for _dict_from_pickle."""

    def test_local(
        self,
        file_pkl: str,
        schema_dict: Dict[str, Dict[str, Dict[str, DataFrameSchema]]],
        bucket_name: Optional[str] = None,
        object_key: Optional[str] = None,
    ) -> None:
        """Path but no bucket or key returns dictionary."""
        returned = _dict_from_pickle(
            bucket_name=bucket_name,
            object_key=object_key,
            file_path=file_pkl,
        )

        assert returned == schema_dict

    def test_s3(
        self,
        test_s3_resource,
        schema_dict: Dict[str, Dict[str, Dict[str, DataFrameSchema]]],
        bucket_name: str = "s3-ranch-019",
        object_key: str = "schemas.pkl",
        file_path: Optional[str] = None,
    ) -> None:
        """Bucket and key but no path returns dictionary."""
        test_s3_resource.create_bucket(Bucket=bucket_name)

        test_s3_object = test_s3_resource.Object(
            bucket_name=bucket_name,
            key=object_key,
        )

        pickled_obj: bytes = dumps(schema_dict)

        test_s3_object.put(
            Body=pickled_obj,
        )

        returned = _dict_from_pickle(
            bucket_name=bucket_name,
            object_key=object_key,
            file_path=file_path,
        )

        assert returned == schema_dict


def test__schema_from_dict(schema_dict, schema) -> None:
    """The expected schema is returned from the schema dictionary."""
    _ = _schema_from_dict(
        theme="test_theme",
        indicator="test_indicator",
        stage="test_stage",
        dict=schema_dict,
    )

    assert _ == schema


def test_validate(
    file_pkl: str,
    df_extracted_output: DataFrame,
) -> None:
    """Validating a valid DataFrame returns that DataFrame."""
    _ = validate(
        theme="test_theme",
        indicator="test_indicator",
        stage="test_stage",
        df=df_extracted_output,
        bucket_name=None,
        object_key=None,
        file_path=file_pkl,
    )

    assert_frame_equal(
        left=_,
        right=df_extracted_output,
    )
