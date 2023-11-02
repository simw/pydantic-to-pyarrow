import datetime
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any, Deque, Dict, List, Literal, Optional, Tuple

import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore
import pytest
from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    NaiveDatetime,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    condecimal,
)
from typing_extensions import Annotated

from pydantic_to_pyarrow import SchemaCreationError, get_pyarrow_schema


def _write_pq_and_read(
    objs: List[Dict[str, Any]], schema: pa.Schema
) -> Tuple[pa.Schema, List[Dict[str, Any]]]:
    """
    This helper function takes a list of dictionaries, and transfers
    them through -> pyarrow -> parquet -> pyarrow -> list of dictionaries,
    returning the schema and the list of dictionaries.

    In this way, it can be checked whether the data conversion has
    affected either the schema or the data.
    """
    tbl = pa.Table.from_pylist(objs, schema=schema)
    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "values.parquet"
        pq.write_table(tbl, path)
        new_tbl = pq.read_table(path)

    new_objs = new_tbl.to_pylist()
    return new_tbl.schema, new_objs


def test_simple_types() -> None:
    class SimpleModel(BaseModel):
        a: str
        b: bool
        c: int
        d: float

    expected = pa.schema(
        [
            pa.field("a", pa.string(), nullable=False),
            pa.field("b", pa.bool_(), nullable=False),
            pa.field("c", pa.int64(), nullable=False),
            pa.field("d", pa.float64(), nullable=False),
        ]
    )

    actual = get_pyarrow_schema(SimpleModel)
    assert actual == expected

    objs = [{"a": "a", "b": True, "c": 1, "d": 1.01}]
    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_strict_simple_types() -> None:
    class SimpleModel(BaseModel):
        a: StrictStr
        b: StrictBool
        c: StrictInt
        d: StrictFloat

    expected = pa.schema(
        [
            pa.field("a", pa.string(), nullable=False),
            pa.field("b", pa.bool_(), nullable=False),
            pa.field("c", pa.int64(), nullable=False),
            pa.field("d", pa.float64(), nullable=False),
        ]
    )

    actual = get_pyarrow_schema(SimpleModel)
    assert actual == expected

    objs = [{"a": "a", "b": True, "c": 1, "d": 1.01}]
    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_unknown_type() -> None:
    class SimpleModel(BaseModel):
        a: Deque[int]

    with pytest.raises(SchemaCreationError) as err:
        get_pyarrow_schema(SimpleModel)
    assert "Unknown type" in str(err)


def test_nullable_types() -> None:
    class NullableModel(BaseModel):
        a: str
        b: Optional[str]
        c: int
        d: Optional[int]

    expected = pa.schema(
        [
            pa.field("a", pa.string(), nullable=False),
            pa.field("b", pa.string(), nullable=True),
            pa.field("c", pa.int64(), nullable=False),
            pa.field("d", pa.int64(), nullable=True),
        ]
    )

    actual = get_pyarrow_schema(NullableModel)
    assert actual == expected

    objs = [{"a": "a", "b": "b", "c": 1, "d": 1}]
    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_date_types_with_no_tz() -> None:
    """
    Dates and naive datetimes are ok to convert to pyarrow and parquet
    without worrying about timezone data.
    """

    class DateModel(BaseModel):
        a: datetime.date
        b: NaiveDatetime

    expected = pa.schema(
        [
            pa.field("a", pa.date32(), nullable=False),
            pa.field("b", pa.timestamp("ms"), nullable=False),
        ]
    )

    actual = get_pyarrow_schema(DateModel)
    assert actual == expected

    objs = [
        {
            "a": datetime.date(2020, 1, 1),
            "b": datetime.datetime(2020, 1, 1, 0, 0, 0),
        }
    ]
    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_date_types_with_tz() -> None:
    """
    datetime.datetime might have timezone data, and AwareDatetime
    certainly should - hence, need to be careful when converting
    to pyarrow and parquet.
    """

    class DateModel(BaseModel):
        a: datetime.datetime
        b: AwareDatetime

    expected = pa.schema(
        [
            pa.field("a", pa.timestamp("ms"), nullable=False),
            pa.field("b", pa.timestamp("ms"), nullable=False),
        ]
    )

    actual = get_pyarrow_schema(DateModel, allow_losing_tz=True)
    assert actual == expected

    tz = datetime.timezone(datetime.timedelta(hours=5))
    objs = [
        {
            "a": datetime.datetime(2020, 1, 1, 1, 0, 0),
            "b": datetime.datetime(2020, 1, 1, 2, 0, 0, tzinfo=tz),
        },
        {
            "a": datetime.datetime(2020, 1, 1, 1, 0, 0, tzinfo=tz),
            "b": datetime.datetime(2020, 1, 1, 6, 0, 0, tzinfo=tz),
        },
    ]
    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    # pyarrow converts to UTC, so the datetimes should be different
    # Winter => New York is UTC-5
    expected_objs = [
        {
            "a": datetime.datetime(2020, 1, 1, 1, 0, 0),
            "b": datetime.datetime(2019, 12, 31, 21, 0, 0),
        },
        {
            "a": datetime.datetime(2019, 12, 31, 20, 0, 0),
            "b": datetime.datetime(2020, 1, 1, 1, 0, 0),
        },
    ]
    assert new_objs == expected_objs


def test_datetime_without_flag() -> None:
    """
    datetime.datetime might have timezone data, hence reject it
    without the extra allow_losing_tz flag.
    """

    class DateModel(BaseModel):
        a: datetime.datetime

    with pytest.raises(SchemaCreationError) as err:
        get_pyarrow_schema(DateModel)
    expected_msg = "only allowed if ok losing timezone information"
    assert expected_msg in str(err)


def test_awaredatetime_without_flag() -> None:
    """
    datetime.datetime might have timezone data, hence reject it
    without the extra allow_losing_tz flag.
    """

    class DateModel(BaseModel):
        a: AwareDatetime

    with pytest.raises(SchemaCreationError) as err:
        get_pyarrow_schema(DateModel)
    expected_msg = "only allowed if ok losing timezone information"
    assert expected_msg in str(err)


def test_decimal() -> None:
    class DecimalModel(BaseModel):
        a: Annotated[Decimal, Field(max_digits=5, decimal_places=2)]
        b: Optional[Annotated[Decimal, Field(max_digits=5, decimal_places=2)]] = None
        # condecimal is discouraged in pydantic 2.x, as it returns a type
        # which doesn't play well with static analysis tools, hence type: ignore
        c: condecimal(max_digits=16, decimal_places=3)  # type: ignore

    expected = pa.schema(
        [
            pa.field("a", pa.decimal128(5, 2), nullable=False),
            pa.field("b", pa.decimal128(5, 2), nullable=True),
            pa.field("c", pa.decimal128(16, 3), nullable=False),
        ]
    )
    actual = get_pyarrow_schema(DecimalModel)
    assert actual == expected

    objs = [
        {
            "a": Decimal("1.23"),
            "b": Decimal("4.56"),
            "c": Decimal("123.456"),
        }
    ]
    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_bare_decimal_should_fail() -> None:
    class DecimalModel(BaseModel):
        a: Decimal

    with pytest.raises(SchemaCreationError) as err:
        get_pyarrow_schema(DecimalModel)
    expected_msg = "Decimal type needs annotation setting max_digits and decimal_places"
    assert expected_msg in str(err)


def test_nested_model() -> None:
    class NestedModel(BaseModel):
        a: str
        b: int

    class OuterModel(BaseModel):
        c: NestedModel
        d: Optional[NestedModel]

    nested_fields = pa.struct(
        [
            pa.field("a", pa.string(), nullable=False),
            pa.field("b", pa.int64(), nullable=False),
        ]
    )
    expected = pa.schema(
        [
            pa.field("c", nested_fields, nullable=False),
            pa.field("d", nested_fields, nullable=True),
        ]
    )

    actual = get_pyarrow_schema(OuterModel)
    assert actual == expected

    objs = [
        {
            "c": {"a": "a", "b": 1},
            "d": {"a": "b", "b": 2},
        }
    ]
    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_literal() -> None:
    class LiteralModel(BaseModel):
        a: Literal["a", "b"]
        b: Literal[1, 2]

    expected = pa.schema(
        [
            pa.field("a", pa.dictionary(pa.int32(), pa.string()), nullable=False),
            pa.field("b", pa.int64(), nullable=False),
        ]
    )
    actual = get_pyarrow_schema(LiteralModel)
    assert actual == expected

    objs = [{"a": "a", "b": 1}]
    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_float_literal() -> None:
    class LiteralModel(BaseModel):
        a: Literal[b"a", b"b"]

    with pytest.raises(SchemaCreationError) as err:
        get_pyarrow_schema(LiteralModel)
    assert "Literal type is only supported with all" in str(err)
