import datetime
import types
from decimal import Decimal
from enum import EnumMeta
from typing import Any, List, Literal, Optional, Type, TypeVar, Union, cast

import pyarrow as pa  # type: ignore
from annotated_types import Ge, Gt
from pydantic import AwareDatetime, BaseModel, NaiveDatetime
from typing_extensions import Annotated, get_args, get_origin

BaseModelType = TypeVar("BaseModelType", bound=BaseModel)
EnumType = TypeVar("EnumType", bound=EnumMeta)


class SchemaCreationError(Exception):
    """Error when creating pyarrow schema."""


FIELD_MAP = {
    str: pa.string(),
    bytes: pa.binary(),
    bool: pa.bool_(),
    float: pa.float64(),
    datetime.date: pa.date32(),
    NaiveDatetime: pa.timestamp("ms", tz=None),
    datetime.time: pa.time64("us"),
}

# Timezone aware datetimes will lose their timezone information
# (but be correctly converted to UTC) when converted to pyarrow.
# Pyarrow does support having an entire column in a single timezone,
# but these bare types cannot guarantee that.
LOSING_TZ_TYPES = {
    datetime.datetime: pa.timestamp("ms", tz=None),
    AwareDatetime: pa.timestamp("ms", tz=None),
}


def _get_int_type(metadata: List[Any]) -> pa.DataType:
    min_value: Optional[int] = None
    for el in metadata:
        if isinstance(el, Gt):
            if el.gt is not None and not isinstance(el.gt, int):
                raise SchemaCreationError("Gt metadata must be int")
            min_value = el.gt
        elif isinstance(el, Ge):
            if el.ge is not None and not isinstance(el.ge, int):
                raise SchemaCreationError("Ge metadata must be int")
            min_value = el.ge

    if min_value is not None and min_value >= 0:
        return pa.uint64()
    return pa.int64()


def _get_decimal_type(metadata: List[Any]) -> pa.DataType:
    general_metadata = None
    for el in metadata:
        if hasattr(el, "max_digits") and hasattr(el, "decimal_places"):
            general_metadata = el
    if general_metadata is None:
        raise SchemaCreationError(
            "Decimal type needs annotation setting max_digits and decimal_places"
        )

    return pa.decimal128(general_metadata.max_digits, general_metadata.decimal_places)


TYPES_WITH_METADATA = {
    Decimal: _get_decimal_type,
    int: _get_int_type,
}


def _get_literal_type(
    field_type: Type[Any],
    _metadata: List[Any],
    _allow_losing_tz: bool,
    _exclude_fields: bool,
) -> pa.DataType:
    values = get_args(field_type)
    if all(isinstance(value, str) for value in values):
        return pa.dictionary(pa.int32(), pa.string())
    elif all(isinstance(value, int) for value in values):
        # Dictionary of (int, int) is converted to just int when
        # written into parquet.
        return pa.int64()
    else:
        msg = "Literal type is only supported with all int or string values. "
        raise SchemaCreationError(msg)


def _get_list_type(
    field_type: Type[Any],
    metadata: List[Any],
    allow_losing_tz: bool,
    _exclude_fields: bool,
) -> pa.DataType:
    sub_type = get_args(field_type)[0]
    if _is_optional(sub_type):
        # pyarrow lists can have null elements in them
        sub_type = list(set(get_args(sub_type)) - {type(None)})[0]
    return pa.list_(
        _get_pyarrow_type(sub_type, metadata, allow_losing_tz, _exclude_fields)
    )


def _get_annotated_type(
    field_type: Type[Any],
    metadata: List[Any],
    allow_losing_tz: bool,
    exclude_fields: bool,
) -> pa.DataType:
    # TODO: fix / clean up / understand why / if this works in all cases
    args = get_args(field_type)[1:]
    metadatas = [
        item.metadata if hasattr(item, "metadata") else [item] for item in args
    ]
    metadata = [item for sublist in metadatas for item in sublist]
    field_type = cast(Type[Any], get_args(field_type)[0])
    return _get_pyarrow_type(field_type, metadata, allow_losing_tz, exclude_fields)


def _get_dict_type(
    field_type: Type[Any],
    metadata: List[Any],
    allow_losing_tz: bool,
    _exclude_fields: bool,
) -> pa.DataType:
    key_type, value_type = get_args(field_type)
    return pa.map_(
        _get_pyarrow_type(
            key_type,
            metadata,
            allow_losing_tz=allow_losing_tz,
            exclude_fields=_exclude_fields,
        ),
        _get_pyarrow_type(
            value_type,
            metadata,
            allow_losing_tz=allow_losing_tz,
            exclude_fields=_exclude_fields,
        ),
    )


FIELD_TYPES = {
    Literal: _get_literal_type,
    list: _get_list_type,
    Annotated: _get_annotated_type,
    dict: _get_dict_type,
}


def _get_enum_type(field_type: Type[Any]) -> pa.DataType:
    is_str = [isinstance(enum_value.value, str) for enum_value in field_type]
    if all(is_str):
        return pa.dictionary(pa.int32(), pa.string())

    is_int = [isinstance(enum_value.value, int) for enum_value in field_type]
    if all(is_int):
        return pa.int64()

    msg = "Enums only allowed if all str or all int"
    raise SchemaCreationError(msg)


def _is_optional(field_type: Type[Any]) -> bool:
    origin = get_origin(field_type)
    is_python_39_union = origin is Union
    is_python_310_union = hasattr(types, "UnionType") and origin is types.UnionType

    if not is_python_39_union and not is_python_310_union:
        return False

    return type(None) in get_args(field_type)


def _get_pyarrow_type(
    field_type: Type[Any],
    metadata: List[Any],
    allow_losing_tz: bool,
    exclude_fields: bool,
) -> pa.DataType:
    if field_type in FIELD_MAP:
        return FIELD_MAP[field_type]

    if allow_losing_tz and field_type in LOSING_TZ_TYPES:
        return LOSING_TZ_TYPES[field_type]

    if not allow_losing_tz and field_type in LOSING_TZ_TYPES:
        raise SchemaCreationError(
            f"{field_type} only allowed if ok losing timezone information"
        )

    if isinstance(field_type, EnumMeta):
        return _get_enum_type(field_type)

    if field_type in TYPES_WITH_METADATA:
        return TYPES_WITH_METADATA[field_type](metadata)

    if get_origin(field_type) in FIELD_TYPES:
        return FIELD_TYPES[get_origin(field_type)](
            field_type, metadata, allow_losing_tz, exclude_fields
        )

    # isinstance(filed_type, type) checks whether it's a class
    # otherwise eg Deque[int] would casue an exception on issubclass
    if isinstance(field_type, type) and issubclass(field_type, BaseModel):
        return _get_pyarrow_schema(
            field_type, allow_losing_tz, exclude_fields, as_schema=False
        )

    raise SchemaCreationError(f"Unknown type: {field_type}")


def _get_pyarrow_schema(
    pydantic_class: Type[BaseModelType],
    allow_losing_tz: bool,
    exclude_fields: bool,
    as_schema: bool = True,
) -> pa.Schema:
    fields = []
    for name, field_info in pydantic_class.model_fields.items():
        if field_info.exclude and exclude_fields:
            continue
        field_type = field_info.annotation
        metadata = field_info.metadata

        if field_type is None:
            # Not sure how to get here through pydantic, hence nocover
            raise SchemaCreationError(
                f"Missing type for field {name}"
            )  # pragma: no cover

        try:
            nullable = False
            if _is_optional(field_type):
                nullable = True
                types_under_union = list(set(get_args(field_type)) - {type(None)})
                # mypy infers field_type as Type[Any] | None here, hence casting
                field_type = cast(Type[Any], types_under_union[0])

            pa_field = _get_pyarrow_type(
                field_type,
                metadata,
                allow_losing_tz=allow_losing_tz,
                exclude_fields=exclude_fields,
            )
        except Exception as err:  # noqa: BLE001 - ignore blind exception
            raise SchemaCreationError(
                f"Error processing field {name}: {field_type}, {err}"
            ) from err

        fields.append(pa.field(name, pa_field, nullable=nullable))

    if as_schema:
        return pa.schema(fields)
    return pa.struct(fields)


def get_pyarrow_schema(
    pydantic_class: Type[BaseModelType],
    allow_losing_tz: bool = False,
    exclude_fields: bool = False,
) -> pa.Schema:
    """
    Converts a Pydantic model into a PyArrow schema.

    Args:
        pydantic_class (Type[BaseModelType]): The Pydantic model class to convert.
        allow_losing_tz (bool, optional): Whether to allow losing timezone information
        when converting datetime fields. Defaults to False.
        exclude_fields (bool, optional): If True, will exclude fields in the pydantic
        model that have `Field(exclude=True)`. Defaults to False.

    Returns:
        pa.Schema: The PyArrow schema representing the Pydantic model.
    """
    return _get_pyarrow_schema(
        pydantic_class, allow_losing_tz, exclude_fields=exclude_fields
    )
