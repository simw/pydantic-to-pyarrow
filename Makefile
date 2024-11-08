.DEFAULT_TARGET: help
sources = src tests


.PHONY: prepare
prepare:
	poetry install


.PHONY: lintable
lintable: prepare
	poetry run black $(sources)
	poetry run ruff --fix $(sources)


.PHONY: lint
lint: prepare
	poetry check
	poetry run black --check --diff $(sources)
	poetry run ruff check $(sources)
	poetry run mypy $(sources)



.PHONY: test
test: prepare
	poetry run coverage run -m pytest
	poetry run coverage report


.PHONY: test-dep-versions
test-dep-versions: prepare
	nox


.PHONY: clean
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]'`
	rm -f `find . -type f -name '*~'`
	rm -f `find . -type f -name '.*~'`
	rm -rf .cache
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf htmlcov
	rm -rf *.egg-info
	rm -f .coverage
	rm -f .coverage.*
	rm -rf build
	rm -rf dist
	rm -rf coverage.xml


.PHONY: package
package: prepare
	poetry build


.PHONY: help
help:
	@grep -E \
		'^.PHONY: .*?## .*$$' $(MAKEFILE_LIST) | \
		sort | \
		awk 'BEGIN {FS = ".PHONY: |## "}; {printf "\033[36m%-19s\033[0m %s\n", $$2, $$3}'
