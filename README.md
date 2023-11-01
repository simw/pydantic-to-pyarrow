# pydantic-to-pyarrow

[![CI](https://github.com/simw/pydantic-to-pyarrow/actions/workflows/test.yml/badge.svg?event=push)](https://github.com/simw/pydantic-to-pyarrow/actions/workflows/test.yml)

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

This library is not yet availabe on PyPI.

## An Example

```py
from typing import Optional

from pydantic import BaseModel
from pydantic_to_pyarrow import get_pyarrow_schema


class MyModel(BaseModel):
    int_field: int
    opt_str_field: Optional[str]
    py310_opt_str_field: str | None


pa_schema = get_pyarrow_schema(MyModel)
print(pa_schema)
#> int_field: int64 not null
#> opt_str_field: string
#> py310_opt_str_field: string
```

## Development

Prerequisites:

- Any Python 3.8 through 3.11
- [poetry](https://github.com/python-poetry/poetry) for dependency management
- git
- make
