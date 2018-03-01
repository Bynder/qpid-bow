.PHONY: all
all: clean lint test dist

.PHONY: clean
clean:
	@echo ">> Cleaning"
	@rm -rf build dist

.PHONY: lint
lint: clean
	@echo ">> Linting"
	@pylint qpid_bow
	-@bandit -r .
	@mypy --ignore-missing-imports qpid_bow

.PHONY: test
test: clean
	@echo ">> Testing"
	@pytest

.PHONY: dist
dist: clean
	@echo ">> Building"
	@python setup.py bdist_wheel
	@echo "!! Build ready"

.PHONY: docs
docs: clean
	@echo ">> Building docs"
	@sphinx-apidoc -f -o docs/source qpid_bow
	@sphinx-build -c docs/source docs/source docs/build
