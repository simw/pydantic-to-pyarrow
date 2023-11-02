import datetime
import types
from decimal import Decimal
from typing import Any, List, Literal, Type, TypeVar, Union, cast

import pyarrow as pa  # type: ignore
from pydantic import AwareDatetime, BaseModel, NaiveDatetime
from typing_extensions import Annotated, get_args, get_origin

BaseModelType = TypeVar("BaseModelType", bound=BaseModel)


class SchemaCreationError(Exception):
    """Error when creating pyarrow schema."""


FIELD_MAP = {
    str: pa.string(),
    bool: pa.bool_(),
    int: pa.int64(),
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
}


def _get_literal_type(
    field_type: type[Any], _metadata: List[Any], _allow_losing_tz: bool
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
    field_type: type[Any], metadata: List[Any], allow_losing_tz: bool
) -> pa.DataType:
    sub_type = get_args(field_type)[0]
    if _is_optional(sub_type):
        # pyarrow lists can have null elements in them
        sub_type = list(set(get_args(sub_type)) - {type(None)})[0]
    return pa.list_(_get_pyarrow_type(sub_type, metadata, allow_losing_tz))


FIELD_TYPES = {
    Literal: _get_literal_type,
    list: _get_list_type,
}


def _is_optional(field_type: type[Any]) -> bool:
    origin = get_origin(field_type)
    is_python_39_union = origin is Union
    is_python_310_union = hasattr(types, "UnionType") and origin is types.UnionType

    if not is_python_39_union and not is_python_310_union:
        return False

    return type(None) in get_args(field_type)


def _get_pyarrow_type(
    field_type: type[Any], metadata: List[Any], allow_losing_tz: bool
) -> pa.DataType:
    if get_origin(field_type) is Annotated:
        # For a 'bare' annotation, the metadata will be
        # supplied directly in field.metadata. However,
        # for lists of annotated types or optional annotated
        # types, we need an extra step to get the metadata.
        metadata = [
            item
            for arg in get_args(field_type)
            if hasattr(arg, "metadata")
            for item in arg.metadata
        ]
        field_type = cast(type[Any], get_args(field_type)[0])

    if field_type in FIELD_MAP:
        return FIELD_MAP[field_type]

    if allow_losing_tz and field_type in LOSING_TZ_TYPES:
        return LOSING_TZ_TYPES[field_type]

    if not allow_losing_tz and field_type in LOSING_TZ_TYPES:
        raise SchemaCreationError(
            f"{field_type} only allowed if ok losing timezone information"
        )

    if field_type in TYPES_WITH_METADATA:
        return TYPES_WITH_METADATA[field_type](metadata)

    if get_origin(field_type) in FIELD_TYPES:
        return FIELD_TYPES[get_origin(field_type)](
            field_type, metadata, allow_losing_tz
        )

    # isinstance(filed_type, type) checks whether it's a class
    # otherwise eg Deque[int] would casue an exception on issubclass
    if isinstance(field_type, type) and issubclass(field_type, BaseModel):
        return _get_pyarrow_schema(field_type, allow_losing_tz, as_schema=False)

    raise SchemaCreationError(f"Unknown type: {field_type}")


def _get_pyarrow_schema(
    pydantic_class: Type[BaseModelType],
    allow_losing_tz: bool,
    as_schema: bool = True,
) -> pa.Schema:
    fields = []
    for name, field_info in pydantic_class.model_fields.items():
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
                # mypy infers field_type as type[Any] | None here, hence casting
                field_type = cast(type[Any], types_under_union[0])

            pa_field = _get_pyarrow_type(
                field_type, metadata, allow_losing_tz=allow_losing_tz
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
    pydantic_class: Type[BaseModelType], allow_losing_tz: bool = False
) -> pa.Schema:
    return _get_pyarrow_schema(pydantic_class, allow_losing_tz)
