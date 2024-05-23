# pydantic-to-pyarrow

[![CI](https://github.com/simw/pydantic-to-pyarrow/actions/workflows/test.yml/badge.svg?event=push)](https://github.com/simw/pydantic-to-pyarrow/actions/workflows/test.yml)
[![pypi](https://img.shields.io/pypi/v/pydantic-to-pyarrow.svg)](https://pypi.python.org/pypi/pydantic-to-pyarrow)
[![versions](https://img.shields.io/pypi/pyversions/pydantic-to-pyarrow.svg)](https://github.com/simw/pydantic-to-pyarrow)
[![license](https://img.shields.io/github/license/simw/pydantic-to-pyarrow.svg)](https://github.com/simw/pydantic-to-pyarrow/blob/main/LICENSE)

pydantic-to-pyarrow is a library for Python to help with conversion
of pydantic models to pyarrow schemas.

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
application could benefit from converting the pydantic model to a
pyarrow schema. This library aims to achieve that.

## Installation

```bash
pip install pydantic-to-pyarrow
```

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

If a field is marked as exclude, (`Field(exclude=True)`), then it will be excluded
from the pyarrow schema if exclude_fields is set to True.

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

- Any Python 3.8 through 3.11
- [poetry](https://github.com/python-poetry/poetry) for dependency management
- git
- make
