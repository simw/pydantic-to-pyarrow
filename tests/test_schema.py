import datetime
import tempfile
import uuid
from decimal import Decimal
from enum import Enum, auto
from pathlib import Path
from typing import Any, Deque, Dict, List, Literal, Optional, Tuple

import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore
import pydantic
import pytest
from annotated_types import Gt
from packaging import version
from pydantic import BaseModel, ConfigDict, Field, PlainSerializer
from pydantic.types import (
    UUID1,
    UUID3,
    UUID4,
    UUID5,
    AwareDatetime,
    NaiveDatetime,
    PositiveInt,
    StrictBool,
    StrictBytes,
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
        e: bytes

    expected = pa.schema(
        [
            pa.field("a", pa.string(), nullable=False),
            pa.field("b", pa.bool_(), nullable=False),
            pa.field("c", pa.int64(), nullable=False),
            pa.field("d", pa.float64(), nullable=False),
            pa.field("e", pa.binary(), nullable=False),
        ]
    )

    actual = get_pyarrow_schema(SimpleModel)
    assert actual == expected

    objs = [{"a": "a", "b": True, "c": 1, "d": 1.01, "e": b"e"}]
    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_strict_simple_types() -> None:
    class SimpleModel(BaseModel):
        a: StrictStr
        b: StrictBool
        c: StrictInt
        d: StrictFloat
        e: StrictBytes

    expected = pa.schema(
        [
            pa.field("a", pa.string(), nullable=False),
            pa.field("b", pa.bool_(), nullable=False),
            pa.field("c", pa.int64(), nullable=False),
            pa.field("d", pa.float64(), nullable=False),
            pa.field("e", pa.binary(), nullable=False),
        ]
    )

    actual = get_pyarrow_schema(SimpleModel)
    assert actual == expected

    objs = [{"a": "a", "b": True, "c": 1, "d": 1.01, "e": b"e"}]
    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_unknown_type() -> None:
    class SimpleModel(BaseModel):
        a: Deque[int]

    with pytest.raises(SchemaCreationError) as err:
        get_pyarrow_schema(SimpleModel)
    assert "Unknown type" in str(err)


def test_positive_ints() -> None:
    class IntModel(BaseModel):
        a: int
        b: PositiveInt
        c: Annotated[int, Field(ge=0)]
        d: Annotated[int, Field(ge=-1)]
        e: Optional[PositiveInt]
        f: List[PositiveInt]

    expected = pa.schema(
        [
            pa.field("a", pa.int64(), nullable=False),
            pa.field("b", pa.uint64(), nullable=False),
            pa.field("c", pa.uint64(), nullable=False),
            pa.field("d", pa.int64(), nullable=False),
            pa.field("e", pa.uint64(), nullable=True),
            pa.field("f", pa.list_(pa.uint64()), nullable=False),
        ]
    )

    actual = get_pyarrow_schema(IntModel)
    assert actual == expected

    objs = [{"a": 1, "b": 1, "c": 1, "d": 1, "e": 1, "f": [1, 2, 3]}]
    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_ints_with_wrong_annotations() -> None:
    class IntModel(BaseModel):
        a: Annotated[int, Gt("5")]

    with pytest.raises(SchemaCreationError) as err:
        get_pyarrow_schema(IntModel)
    assert "Gt metadata must be int" in str(err)

    class IntModel2(BaseModel):
        a: Annotated[int, Field(ge="5")]

    with pytest.raises(SchemaCreationError) as err:
        get_pyarrow_schema(IntModel2)
    assert "Ge metadata must be int" in str(err)


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


def test_list_of_strings() -> None:
    class ListModel(BaseModel):
        a: List[str]

    expected = pa.schema(
        [
            pa.field("a", pa.list_(pa.string()), nullable=False),
        ]
    )
    actual = get_pyarrow_schema(ListModel)
    assert actual == expected

    objs = [{"a": ["a", "b"]}]
    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_list_of_decimals() -> None:
    class ListModel(BaseModel):
        a: List[Annotated[Decimal, Field(max_digits=5, decimal_places=2)]]

    expected = pa.schema(
        [
            pa.field("a", pa.list_(pa.decimal128(5, 2)), nullable=False),
        ]
    )

    actual = get_pyarrow_schema(ListModel)
    assert actual == expected

    objs = [{"a": [Decimal("1.23"), Decimal("4.56")]}]
    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_list_of_optional_elements() -> None:
    class ListModel(BaseModel):
        a: List[Optional[str]]
        b: List[Optional[Annotated[Decimal, Field(max_digits=5, decimal_places=2)]]]

    expected = pa.schema(
        [
            pa.field("a", pa.list_(pa.string()), nullable=False),
            pa.field("b", pa.list_(pa.decimal128(5, 2)), nullable=False),
        ]
    )
    actual = get_pyarrow_schema(ListModel)
    assert actual == expected

    objs = [{"a": ["a", None], "b": [Decimal("1.23"), None]}]
    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_enum_str() -> None:
    class MyEnum(Enum):
        val1 = "val1"
        val2 = "val2"
        val3 = "val3"

    class EnumModel(BaseModel):
        a: MyEnum
        b: List[MyEnum]
        c: Optional[MyEnum]

    expected = pa.schema(
        [
            pa.field("a", pa.dictionary(pa.int32(), pa.string()), nullable=False),
            pa.field(
                "b", pa.list_(pa.dictionary(pa.int32(), pa.string())), nullable=False
            ),
            pa.field("c", pa.dictionary(pa.int32(), pa.string()), nullable=True),
        ]
    )

    actual = get_pyarrow_schema(EnumModel)
    assert actual == expected

    objs = [{"a": "val1", "b": ["val2", "val3"], "c": None}]
    model = EnumModel.model_validate(objs[0])
    assert model.a == MyEnum.val1
    assert model.b == [MyEnum.val2, MyEnum.val3]
    assert model.c is None

    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_enum_int() -> None:
    class MyEnum(Enum):
        val1 = 1
        val2 = 2
        val3 = auto()

    class EnumModel(BaseModel):
        a: MyEnum
        b: List[MyEnum]
        c: Optional[MyEnum]

    expected = pa.schema(
        [
            pa.field("a", pa.int64(), nullable=False),
            pa.field("b", pa.list_(pa.int64()), nullable=False),
            pa.field("c", pa.int64(), nullable=True),
        ]
    )

    actual = get_pyarrow_schema(EnumModel)
    assert actual == expected

    objs = [{"a": 1, "b": [2, 3], "c": None}]
    model = EnumModel.model_validate(objs[0])
    assert model.a == MyEnum.val1
    assert model.b == [MyEnum.val2, MyEnum.val3]
    assert model.c is None

    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected
    assert new_objs == objs


def test_enum_mixed() -> None:
    class MyEnum(Enum):
        val1 = 1
        val2 = "val2"

    class EnumModel(BaseModel):
        a: MyEnum

    with pytest.raises(SchemaCreationError):
        get_pyarrow_schema(EnumModel)


def test_exclude_field_true() -> None:
    class SimpleModel(BaseModel):
        a: str
        b: str = Field(exclude=True)

    expected = pa.schema(
        [
            pa.field("a", pa.string(), nullable=False),
        ]
    )

    actual = get_pyarrow_schema(SimpleModel, exclude_fields=True)

    assert actual == expected


def test_exclude_fields_false() -> None:
    class SimpleModel(BaseModel):
        a: str
        b: str = Field(exclude=True)

    expected = pa.schema(
        [
            pa.field("a", pa.string(), nullable=False),
            pa.field("b", pa.string(), nullable=False),
        ]
    )

    actual = get_pyarrow_schema(SimpleModel)

    assert actual == expected


def test_dict() -> None:
    class DictModel(BaseModel):
        foo: Dict[str, int]

    expected = pa.schema(
        [
            pa.field("foo", pa.map_(pa.string(), pa.int64()), nullable=False),
        ]
    )

    objs = [
        {"foo": {"a": 1, "b": 2}},
        {"foo": {"c": 3, "d": 4, "e": 5}},
    ]

    actual = get_pyarrow_schema(DictModel)
    assert actual == expected

    new_schema, new_objs = _write_pq_and_read(objs, expected)
    assert new_schema == expected

    # pyarrow converts to tuples, need to convert back to dicts
    assert objs == [{"foo": dict(t["foo"])} for t in new_objs]


def test_uuid() -> None:
    # pyarrow 18.0.0+ is required for UUID support
    # Even then, pyarrow doesn't automatically convert UUIDs to bytes
    # for the serialization, so we need to do that manually
    # (https://github.com/apache/arrow/issues/43855)
    as_bytes = PlainSerializer(lambda x: x.bytes, return_type=bytes)

    class ModelWithUUID(BaseModel):
        foo_0: Annotated[uuid.UUID, as_bytes] = Field(default_factory=uuid.uuid1)
        foo_1: Annotated[UUID1, as_bytes] = Field(default_factory=uuid.uuid1)
        foo_3: Annotated[UUID3, as_bytes] = Field(
            default_factory=lambda: uuid.uuid3(uuid.NAMESPACE_DNS, "pydantic.org")
        )
        foo_4: Annotated[UUID4, as_bytes] = Field(default_factory=uuid.uuid4)
        foo_5: Annotated[UUID5, as_bytes] = Field(
            default_factory=lambda: uuid.uuid5(uuid.NAMESPACE_DNS, "pydantic.org")
        )

    if version.Version(pa.__version__) < version.Version("18.0.0"):
        with pytest.raises(SchemaCreationError) as err:
            get_pyarrow_schema(ModelWithUUID)
        assert "needs version 18.0 or higher" in str(err)
    else:
        expected = pa.schema(
            [
                pa.field("foo_0", pa.uuid(), nullable=False),
                pa.field("foo_1", pa.uuid(), nullable=False),
                pa.field("foo_3", pa.uuid(), nullable=False),
                pa.field("foo_4", pa.uuid(), nullable=False),
                pa.field("foo_5", pa.uuid(), nullable=False),
            ]
        )

        actual = get_pyarrow_schema(ModelWithUUID)
        assert actual == expected

        objs = [
            ModelWithUUID().model_dump(),
            ModelWithUUID().model_dump(),
        ]

        new_schema, new_objs = _write_pq_and_read(objs, expected)
        assert new_schema == expected
        # objs was created with the uuid serializer to bytes,
        # but pyarrow will read the uuids into UUID objects directly
        for obj in objs:
            for key in obj:
                obj[key] = uuid.UUID(bytes=obj[key])
        assert new_objs == objs


def test_alias() -> None:
    class AliasModel(BaseModel):
        field1: str
        field2: str = Field(alias="b2")
        field3: str = Field(validation_alias="b3")
        field4: str = Field(serialization_alias="b4")
        field5: str = Field(alias="b5", serialization_alias="c5")
        field6: Annotated[str, Field(alias="b6")]

    expected_no_alias = pa.schema(
        [
            pa.field("field1", pa.string(), nullable=False),
            pa.field("field2", pa.string(), nullable=False),
            pa.field("field3", pa.string(), nullable=False),
            pa.field("field4", pa.string(), nullable=False),
            pa.field("field5", pa.string(), nullable=False),
            pa.field("field6", pa.string(), nullable=False),
        ]
    )

    actual_no_alias = get_pyarrow_schema(AliasModel)
    assert actual_no_alias == expected_no_alias

    expected_by_alias = pa.schema(
        [
            pa.field("field1", pa.string(), nullable=False),
            pa.field("b2", pa.string(), nullable=False),
            pa.field("field3", pa.string(), nullable=False),
            pa.field("b4", pa.string(), nullable=False),
            pa.field("c5", pa.string(), nullable=False),
            pa.field("b6", pa.string(), nullable=False),
        ]
    )
    actual_by_alias = get_pyarrow_schema(AliasModel, by_alias=True)
    assert actual_by_alias == expected_by_alias


def test_alias_generator() -> None:
    class AliasModel(BaseModel):
        model_config = ConfigDict(alias_generator=lambda field_name: field_name.upper())
        field1: str
        field2: str = Field(alias="b2")
        field3: str = Field(validation_alias="b3")
        field4: str = Field(serialization_alias="b4")
        field5: str = Field(alias="b5", serialization_alias="c5")
        field6: str = Field(alias="b6", alias_priority=1)

    expected_no_alias = pa.schema(
        [
            pa.field("field1", pa.string(), nullable=False),
            pa.field("field2", pa.string(), nullable=False),
            pa.field("field3", pa.string(), nullable=False),
            pa.field("field4", pa.string(), nullable=False),
            pa.field("field5", pa.string(), nullable=False),
            pa.field("field6", pa.string(), nullable=False),
        ]
    )

    actual_no_alias = get_pyarrow_schema(AliasModel)
    assert actual_no_alias == expected_no_alias

    expected_by_alias = pa.schema(
        [
            pa.field("FIELD1", pa.string(), nullable=False),
            pa.field("b2", pa.string(), nullable=False),
            pa.field("FIELD3", pa.string(), nullable=False),
            pa.field("b4", pa.string(), nullable=False),
            pa.field("c5", pa.string(), nullable=False),
            pa.field("FIELD6", pa.string(), nullable=False),
        ]
    )

    pydantic_version = version.parse(pydantic.__version__)
    if pydantic_version < version.parse("2.5.0"):
        # pydantic 2.5.0 fixed an issue / bug, that setting validation_alias
        # would then remove the alias_generator from the serialization_alias
        # This library follows the functionality in the installed version
        # of pydantic.
        expected_by_alias = pa.schema(
            [
                pa.field("FIELD1", pa.string(), nullable=False),
                pa.field("b2", pa.string(), nullable=False),
                pa.field("field3", pa.string(), nullable=False),
                pa.field("b4", pa.string(), nullable=False),
                pa.field("c5", pa.string(), nullable=False),
                pa.field("FIELD6", pa.string(), nullable=False),
            ]
        )

    actual_by_alias = get_pyarrow_schema(AliasModel, by_alias=True)
    assert actual_by_alias == expected_by_alias
