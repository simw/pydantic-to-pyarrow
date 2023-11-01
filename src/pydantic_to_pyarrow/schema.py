import datetime
import types
from typing import Any, Type, TypeVar, Union, cast

import pyarrow as pa  # type: ignore
from pydantic import BaseModel, NaiveDatetime
from typing_extensions import get_args, get_origin

BaseModelType = TypeVar("BaseModelType", bound=BaseModel)


FIELD_MAP = {
    str: pa.string(),
    bool: pa.bool_(),
    int: pa.int64(),
    float: pa.float64(),
    datetime.date: pa.date32(),
    NaiveDatetime: pa.timestamp("ms", tz=None),
}


class SchemaCreationError(Exception):
    """Error when creating pyarrow schema."""


def _is_optional(field_type: type[Any]) -> bool:
    origin = get_origin(field_type)
    is_python_39_union = origin is Union
    is_python_310_union = hasattr(types, "UnionType") and origin is types.UnionType

    if not is_python_39_union and not is_python_310_union:
        return False

    return type(None) in get_args(field_type)


def _get_pyarrow_type(field_type: type[Any]) -> pa.DataType:
    if field_type in FIELD_MAP:
        return FIELD_MAP[field_type]

    raise SchemaCreationError(f"Unknown type: {field_type}")


def get_pyarrow_schema(
    pydantic_class: Type[BaseModelType],
) -> pa.Schema:
    fields = []
    for name, field_info in pydantic_class.model_fields.items():
        field_type = field_info.annotation

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

            pa_field = _get_pyarrow_type(field_type)
        except Exception as err:  # noqa: BLE001 - ignore blind exception
            raise SchemaCreationError(
                f"Error processing field {name}: {field_type}, {err}"
            ) from err

        fields.append(pa.field(name, pa_field, nullable=nullable))

    return pa.schema(fields)
