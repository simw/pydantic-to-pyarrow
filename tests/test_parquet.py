import datetime

import pyarrow as pa  # type: ignore
import pytest

from .test_schema import _write_pq_and_read


def test_some_types_dont_read_as_written_part1() -> None:
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


def test_some_types_dont_read_as_written_part2() -> None:
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
