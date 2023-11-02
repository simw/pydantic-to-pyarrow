import datetime

import pyarrow as pa  # type: ignore
import pytest

from .test_schema import _write_pq_and_read


def test_some_types_dont_read_as_written_datetime_s() -> None:
    """
    The pyarrow timestampe with precision of seconds is not
    supported by parquet files, only ms level precision.
    """
    schema = pa.schema(
        [
            pa.field("a", pa.timestamp("s"), nullable=True),
        ]
    )
    objs = [{"a": datetime.datetime(2020, 1, 1)}]
    new_schema, new_objs = _write_pq_and_read(objs, schema)
    assert new_objs == objs
    assert len(new_schema) == 1
    assert new_schema[0] == pa.field("a", pa.timestamp("ms"), nullable=True)
    with pytest.raises(AssertionError):
        assert new_schema == schema


def test_some_types_dont_read_as_written_dict_keys() -> None:
    """
    Dictionary keys are converted to int32 when written to parquet files.
    """
    schema = pa.schema(
        [
            pa.field("a", pa.dictionary(pa.int8(), pa.string()), nullable=True),
        ]
    )
    objs = [{"a": "hello"}]
    new_schema, new_objs = _write_pq_and_read(objs, schema)
    assert new_objs == objs
    assert len(new_schema) == 1
    assert new_schema[0] == pa.field(
        "a", pa.dictionary(pa.int32(), pa.string()), nullable=True
    )
    with pytest.raises(AssertionError):
        assert new_schema == schema


def test_some_types_dont_read_as_written_dict_values_int() -> None:
    """
    If dictionary values are ints, then it just stores them as ints,
    and not as the dictionary.
    """
    schema = pa.schema(
        [
            pa.field("a", pa.dictionary(pa.int8(), pa.int64()), nullable=True),
        ]
    )
    objs = [{"a": 1}]
    new_schema, new_objs = _write_pq_and_read(objs, schema)
    assert new_objs == objs
    assert len(new_schema) == 1
    assert new_schema[0] == pa.field("a", pa.int64(), nullable=True)
    with pytest.raises(AssertionError):
        assert new_schema == schema


def test_some_types_dont_read_as_written_tz_loss() -> None:
    """
    While parquet files should correctly convert from the python timezone
    aware datetime to an 'instant', it doesn't record the timezone in the
    parquet file. Hence, when it's read back, we don't get exactly the
    same datetime object.
    """
    schema = pa.schema(
        [
            pa.field("a", pa.timestamp("ms"), nullable=True),
        ]
    )
    tz = datetime.timezone(datetime.timedelta(hours=5))
    objs = [{"a": datetime.datetime(2020, 1, 1, 1, 0, 0, tzinfo=tz)}]
    new_schema, new_objs = _write_pq_and_read(objs, schema)
    assert new_schema == schema
    # The tzinfo is lost, and the datetime is converted to UTC
    # which pushes it back into the previous year.
    assert new_objs[0]["a"] == datetime.datetime(2019, 12, 31, 20, 0, 0)
    with pytest.raises(AssertionError):
        assert new_objs == objs
