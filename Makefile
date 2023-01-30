all:

PYTHON ?= python3
PYTHON_SOURCE_DIRS = rpm_s3_mirror/ tests/
PYTEST_ARG ?= -v
PYLINT=$(shell which pylint 2> /dev/null || which pylint-3)

clean:
	$(RM) -r *.egg-info/ build/ dist/ rpm/
	$(RM) ../rpm_s3_mirror_* test-*.xml

test: copyright lint unittest

reformat:
	$(PYTHON) -m black tests/ rpm_s3_mirror/

validate-style:
	$(eval CHANGES_BEFORE := $(shell mktemp))
	git diff > $(CHANGES_BEFORE)
	$(MAKE) reformat
	$(eval CHANGES_AFTER := $(shell mktemp))
	git diff > $(CHANGES_AFTER)
	diff $(CHANGES_BEFORE) $(CHANGES_AFTER)
	-rm $(CHANGES_BEFORE) $(CHANGES_AFTER)


.PHONY: copyright
copyright:
	$(eval MISSING_COPYRIGHT := $(shell git ls-files "*.py" | xargs grep -EL "Copyright \(c\) 20.* Aiven|Aiven license OK"))
	@if [ "$(MISSING_COPYRIGHT)" != "" ]; then echo Missing Copyright statement in files: $(MISSING_COPYRIGHT) ; false; fi

unittest:
	$(PYTHON) -m pytest $(PYTEST_ARG) tests/

coverage:
	$(PYTHON) -m coverage run --source rpm_s3_mirror -m pytest $(PYTEST_ARG) tests/
	$(PYTHON) -m coverage report --show-missing

pylint:
	$(PYLINT) --rcfile .pylintrc $(PYTHON_SOURCE_DIRS)

flake8:
	$(PYTHON) -m flake8 --exclude=__init__.py --ignore=E722 --max-line-len=125 $(PYTHON_SOURCE_DIRS)

lint: pylint flake8

.PHONY: rpm
