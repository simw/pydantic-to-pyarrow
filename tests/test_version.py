import pydantic_to_pyarrow


def test_version() -> None:
    assert pydantic_to_pyarrow.__version__ is not None
