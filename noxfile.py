from enum import Enum

import nox


class Result(Enum):
    SUCCESS = "Success"
    FAILURE = "Failure"
    SKIPPED = "Skipped"


PYDANTIC = ["2.0.3", "2.4.2", "2.5.0", "2.9.2"]
PYARROW = [
    "11.0.0",
    "12.0.1",
    "13.0.0",
    "14.0.2",
    "15.0.2",
    "16.1.0",
    "17.0.0",
    "18.0.0",
]
NUMPY = ["1.24.4", "1.26.4", "2.0.2", "2.1.3"]


@nox.session(python=False)
def test_pydantic_versions(session) -> None:
    """
    Pydantic has a change in alias behavior in 2.5.0, where
    the serialization_alias will use the alias_generator even
    when validation_alias is set (before 2.5, the serialization_alias
    would be None unless explicitly set when validation_alias was set, even
    when an alias_generator was set).

    The code tests for this, so running on multiple versions of pydantic
    checks that this is handled correctly.
    """
    session.run("uv", "sync")
    value = session.run("uv", "run", "--no-sync", "python", "--version", silent=True)
    version_list = value.lstrip("Python ").split(".")[0:2]
    version = ".".join(version_list)
    print(f"Python version: {version}")

    results = {}
    for pydantic in PYDANTIC:
        name = f"pydantic=={pydantic}"
        deps = {"pydantic": pydantic}

        result = Result.SKIPPED
        if version < "3.13":
            result = run_tests(session, deps)

        if version == "3.13":
            # pydantic 2.8 is the first version to work with python 3.13
            if pydantic >= "2.8.0":
                result = run_tests(session, deps)

        results[name] = result

    print_results(results)
    if any(result == Result.FAILURE for result in results.values()):
        raise Exception("Test failed")


@nox.session(python=False)
def test_pyarrow_versions(session) -> None:
    session.run("uv", "sync")
    value = session.run("uv", "run", "--no-sync", "python", "--version", silent=True)
    version_list = value.lstrip("Python ").split(".")[0:2]
    version = ".".join(version_list)
    print(f"Python version: {version}")

    results = {}
    for pyarrow in PYARROW:
        for numpy in NUMPY:
            name = f"pyarrow=={pyarrow} & numpy=={numpy}"
            deps = {"pyarrow": pyarrow, "numpy": numpy}

            result = Result.SKIPPED
            if version == "3.8":
                # All dependency constraints correctly expressed by pyarrow and numpy
                # ie numpy < 1.25 and pyarrow < 18
                if pyarrow < "18.0.0" and numpy == "1.24.4":
                    result = run_tests(session, deps)

            if version == "3.9":
                # Pyarrow < 15 does not correctly specify numpy < 2 constraint
                # Pyarrow 15 only runs with numpy 1.x
                # Pyarrow 16 and above can run with numpy 1.x or 2.x
                # Python 3.9 has numpy < 2.1
                if (pyarrow < "16.0.0" and numpy < "2.0") or (
                    pyarrow >= "16.0.0" and numpy < "2.1"
                ):
                    result = run_tests(session, deps)

            if version in ["3.10", "3.11"]:
                # Pyarrow < 15 does not correctly specify numpy < 2 constraint
                # pyarrow 15 only runs with numpy 1.x
                if (pyarrow < "16.0.0" and numpy < "2.0") or pyarrow >= "16.0.0":
                    result = run_tests(session, deps)

            if version == "3.12":
                # Pyarrow < 15 does not correctly specify numpy < 2 constraint
                # pyarrow 15 only runs with numpy 1.x
                # No binary builds for pyarrow < 14 for python 3.12
                # numpy 1.24.4 won't install on python 3.12+
                if not (pyarrow < "16.0.0" and numpy >= "2.0") and (
                    pyarrow > "14.0" and numpy >= "1.25"
                ):
                    result = run_tests(session, deps)

            if version == "3.13":
                # No binary builds for pyarrow < 18 for python 3.13
                # numpy 1.24.4 won't install on python 3.12+
                if pyarrow >= "18.0" and numpy >= "1.25":
                    result = run_tests(session, deps)

            results[name] = result

    print_results(results)
    if any(result == Result.FAILURE for result in results.values()):
        raise Exception("Test failed")


def run_tests(session, deps) -> Result:
    session.run("uv", "sync")
    dep_list = [f"{k}=={v}" for k, v in deps.items()]
    try:
        session.run("uv", "pip", "install", *dep_list)
        session.run("uv", "run", "python", "-m", "pytest")
        return Result.SUCCESS
    except Exception:
        return Result.FAILURE


def print_results(results) -> None:
    for k, v in results.items():
        if v == Result.SUCCESS:
            print(f"\033[32m{k}: {v}\033[0m")
        elif v == Result.FAILURE:
            print(f"\033[31m{k}: {v}\033[0m")
        else:
            print(f"\033[33m{k}: {v}\033[0m")
