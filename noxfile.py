import nox


PYARROW = ["11.0.0", "12.0.1", "13.0.0", "14.0.2", "15.0.2", "16.1.0", "17.0.0", "18.0.0"]
NUMPY = ["1.24.4", "1.26.4", "2.0.2", "2.1.3"]

@nox.session(python=False)
@nox.parametrize("pydantic", ["2.0.3", "2.4.2", "2.9.2"])
def test_pydantic_versions(session, pydantic) -> None:
    """
    Pydantic has a change in alias behavior in 2.5.0, where
    the serialization_alias will use the alias_generator even
    when validation_alias is set (before 2.5, the serialization_alias
    would be None unless explicitly set when validation_alias was set, even
    when an alias_generator was set).

    The code tests for this, so running on multiple versions of pydantic
    checks that this is handled correctly.
    """
    session.run("poetry", "install")
    session.run("poetry", "run", "pip", "install", f"pydantic=={pydantic}")
    session.run("poetry", "run", "python", "-m", "pytest")


@nox.session(python=False)
def test_pyarrow_versions(session) -> None:
    value = session.run("poetry", "run", "python", "--version", silent=True)
    version = ".".join(value.split(" ")[1].split(".")[0:2])
    print(f"Python version: {version}")

    failure = False
    results = {}
    for pyarrow in PYARROW:
        for numpy in NUMPY:
            name = f"pyarrow=={pyarrow} & numpy=={numpy}"
            try:
                if version == "3.8":
                    # All dependency constraints correctly expressed by pyarrow and numpy
                    # ie numpy < 1.25 and pyarrow < 18
                    if pyarrow < "18.0.0" and numpy == "1.24.4":
                        run_tests(session, pyarrow, numpy)

                if version == "3.9":
                    # Pyarrow < 15 does not correctly specify numpy < 2 constraint
                    # Pyarrow 15 only runs with numpy 1.x 
                    # Pyarrow 16 and above can run with numpy 1.x or 2.x
                    # Python 3.9 has numpy < 2.1
                    if (pyarrow < "16.0.0" and numpy < "2.0") or \
                        (pyarrow >= "16.0.0" and numpy < "2.1"):
                        run_tests(session, pyarrow, numpy)

                if version in ["3.10", "3.11"]:
                    # Pyarrow < 15 does not correctly specify numpy < 2 constraint 
                    # pyarrow 15 only runs with numpy 1.x
                    if (pyarrow < "16.0.0" and numpy < "2.0") or pyarrow >= "16.0.0":
                        run_tests(session, pyarrow, numpy)

                if version == "3.12":
                    # Pyarrow < 15 does not correctly specify numpy < 2 constraint 
                    # pyarrow 15 only runs with numpy 1.x
                    if (pyarrow < "16.0.0" and numpy >= "2.0"):
                        continue

                    # No binary builds for pyarrow < 14 for python 3.12
                    # numpy 1.24.4 won't install on python 3.12+
                    if pyarrow > "14.0" and numpy >= "1.25":
                        run_tests(session, pyarrow, numpy)

                if version == "3.13":
                    # No binary builds for pyarrow < 18 for python 3.13
                    # numpy 1.24.4 won't install on python 3.12+
                    if pyarrow >= "18.0" and numpy >= "1.25":
                        run_tests(session, pyarrow, numpy)

                results[name] = "Success"
            except Exception as e:
                results[name] = "Failure"
                failure = True
                continue

    for k, v in results.items():
        print(f"{k}: {v}")
    if failure:
        raise Exception("Test failed")


def run_tests(session, pyarrow, numpy):
    session.run("poetry", "install")
    session.run("poetry", "run", "pip", "install", f"pyarrow=={pyarrow}", f"numpy=={numpy}")
    session.run("poetry", "run", "python", "-m", "pytest")
