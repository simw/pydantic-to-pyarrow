# pydantic-to-pyarrow

[![CI](https://github.com/simw/pydantic-to-pyarrow/actions/workflows/test.yml/badge.svg?event=push)](https://github.com/simw/pydantic-to-pyarrow/actions/workflows/test.yml)
[![pypi](https://img.shields.io/pypi/v/pydantic-to-pyarrow.svg)](https://pypi.python.org/pypi/pydantic-to-pyarrow)
[![versions](https://img.shields.io/pypi/pyversions/pydantic-to-pyarrow.svg)](https://github.com/simw/pydantic-to-pyarrow)
[![license](https://img.shields.io/github/license/simw/pydantic-to-pyarrow.svg)](https://github.com/simw/pydantic-to-pyarrow/blob/main/LICENSE)
[![Download Stats](https://img.shields.io/pypi/dm/pydantic-to-pyarrow)](https://pypistats.org/packages/pydantic-to-pyarrow)

pydantic-to-pyarrow is a library for Python to help with conversion
of pydantic models to pyarrow schemas.

(Please note that this project is not affiliated in any way with the
great teams at [pydantic](https://github.com/pydantic/pydantic) or
[pyarrow](https://github.com/apache/arrow).)

[pydantic](https://github.com/pydantic/pydantic) is a Python library
for data validation, applying type hints / annotations. It enables
the creation of easy or complex data validation rules.

[pyarrow](https://arrow.apache.org/docs/python/index.html) is a Python library
for using Apache Arrow, a development platform for in-memory analytics. The library
also enables easy writing to parquet files.

Why might you want to convert models to schemas? One scenario is for a data
processing pipeline:

1. Import / extract the data from its source
2. Validate the data using pydantic
3. Process the data in pyarrow / pandas / polars
4. Store the raw and / or processed data in parquet.

The easiest approach for steps 3 and 4 above is to let pyarrow infer
the schema from the data. The most involved approach is to
specify the pyarrow schema separate from the pydantic model. In the middle, many
applications could benefit from converting the pydantic model to a
pyarrow schema. This library aims to achieve that.

## Installation

```bash
pip install pydantic-to-pyarrow
```

Note: PyArrow versions < 15 are only compatible with NumPy 1.x, but
they do not express this in their dependency constraints. If other constraints
are forcing you to use PyArrow < 15 on Python 3.9+, and you see errors like
'A module that was compiled using NumPy 1.x cannot be run in Numpy 2.x ...',
then try forcing NumPy 1.x in your project's dependencies.

## Conversion Table

The below conversions still run into the possibility of
overflows in the Pyarrow types. For example, in Python 3
the `int` type is unbounded, whereas the pa.int64() type has a fixed
maximum. In most cases, this should not be an issue, but if you are
concerned about overflows, you should not use this library and
should manually specify the full schema.

Python / Pydantic | Pyarrow | Overflow
--- | --- | ---
str | pa.string() |
Literal[strings] | pa.dictionary(pa.int32(), pa.string()) |
. | . | .
int | pa.int64() if no minimum constraint, pa.uint64() if minimum is zero | Yes, at 2^63 (for signed) or 2^64 (for unsigned)
Literal[ints] | pa.int64() |
float | pa.float64() | Yes
decimal.Decimal | pa.decimal128 ONLY if supplying max_digits and decimal_places for pydantic field | Yes
. | . | .
datetime.date | pa.date32() |
datetime.time | pa.time64("us") |
datetime.datetime | pa.timestamp("ms", tz=None) ONLY if param allow_losing_tz=True |
pydantic.types.NaiveDatetime | pa.timestamp("ms", tz=None) |
pydantic.types.AwareDatetime | pa.timestamp("ms", tz=None) ONLY if param allow_losing_tz=True |
. | .
Optional[...] | The pyarrow field is nullable |
Pydantic Model | pa.struct() |
List[...] | pa.list_(...) |
Dict[..., ...] | pa.map_(pa key_type, pa value_type) |
Enum of str | pa.dictionary(pa.int32(), pa.string()) | 
Enum of int | pa.int64() |
UUID (uuid.UUID or pydantic.types.UUID*) | pa.uuid() | SEE NOTE BELOW!

Note on UUIDs: the UUID type is only supported in pyarrow 18.0 and above. However,
as of pyarrow 19.0, when pyarrow creates a table in eg `pa.Table.from_pylist(objs, schema=schema)`,
it expects bytes not a uuid.UUID type. Hence, if you are using .model_dump() to create
the data for pyarrow, you need to add a serializer on your pydantic model to convert to bytes.
This may be fixed in later versions (see [https://github.com/apache/arrow/issues/43855]).

eg (with pyarrow >= 18.0):
```py
import uuid
from typing import Annotated

import pyarrow as pa
from pydantic import BaseModel, PlainSerializer
from pydantic_to_pyarrow import get_pyarrow_schema

class ModelWithUuid(BaseModel):
    uuid: Annotated[uuid.UUID, PlainSerializer(lambda x: x.bytes, return_type=bytes)]


schema = get_pyarrow_schema(ModelWithUuid)

model1 = ModelWithUuid(uuid=uuid.uuid1())
model2 = ModelWithUuid(uuid=uuid.uuid4())
data = [model1.model_dump(), model2.model_dump()]
table = pa.Table.from_pylist(data)
print(table)
#> pyarrow.Table
#> uuid: binary
#> ----
#> uuid: [[BF206AC0DA4711EF8271EF4F4B7A3587,211C4C5D94C74876AE5E32DBCCDC16C7]]
```

## Settings

In a model, if a field is marked as exclude, `Field(exclude=True)`, then it will be excluded
from the pyarrow schema if `get_pyarrow_schema` is called with `exclude_fields=True` (defaults to False).

If `get_pyarrow_schema` is called with `allow_losing_tz=True`, then it will allow conversion
of timezone-aware python datetimes to non-timezone aware pyarrow timestamps
(defaults to False - and loss of timezone information will raise an exception).

By default, `get_pyarrow_schema` will use the field names for the pyarrow schema fields. If
`by_alias=True` is supplied, then the serialization_alias is used. More information about aliases is available in the [Pydantic documentation](https://docs.pydantic.dev/latest/concepts/alias/).

## An Example

```py
from typing import Dict, List, Optional

from pydantic import BaseModel, Field
from pydantic_to_pyarrow import get_pyarrow_schema

class NestedModel(BaseModel):
    str_field: str


class MyModel(BaseModel):
    int_field: int
    opt_str_field: Optional[str]
    py310_opt_str_field: str | None
    nested: List[NestedModel]
    dict_field: Dict[str, int]
    excluded_field: str = Field(exclude=True)


pa_schema = get_pyarrow_schema(MyModel)
print(pa_schema)
#> int_field: int64 not null
#> opt_str_field: string
#> py310_opt_str_field: string
#> nested: list<item: struct<str_field: string not null>> not null
#>   child 0, item: struct<str_field: string not null>
#>       child 0, str_field: string not null
#> dict_field: map<string, int64> not null
#>   child 0, entries: struct<key: string not null, value: int64> not null
#>       child 0, key: string not null
#>       child 1, value: int64
```

## Development

Prerequisites:

- Any Python 3.8 through 3.13
- [uv](https://github.com/astral-sh/uv) for dependency management
- git
- make
- [nox](https://nox.thea.codes/en/stable/index.html) (to run tests across dependency versions)
