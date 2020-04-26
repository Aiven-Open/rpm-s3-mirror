short_ver = 0.0.1
long_ver = $(shell git describe --long 2>/dev/null || echo $(short_ver)-0-unknown-g`git describe --always`)
generated = rpm_s3_mirror/version.py

all: $(generated)

PYTHON ?= python3
PYTHON_SOURCE_DIRS = rpm_s3_mirror/ test/
PYTEST_ARG ?= -v

clean:
	$(RM) -r *.egg-info/ build/ dist/ rpm/
	$(RM) ../rpm_s3_mirror_* test-*.xml $(generated)

rpm:
	git archive --output=rpm_s3_mirror-rpm-src.tar --prefix=rpm_s3_mirror/ HEAD
	rpmbuild -bb rpm_s3_mirror.spec \
		--define '_topdir $(PWD)/rpm' \
		--define '_sourcedir $(CURDIR)' \
		--define 'major_version $(short_ver)' \
		--define 'minor_version $(subst -,.,$(subst $(short_ver)-,,$(long_ver)))'
	$(RM) rpm_s3_mirror-rpm-src.tar

build-dep-fed:
	sudo dnf -y install --best --allowerasing \
		python3-defusedxml \
		python3-requests \
		python3-dateutil \
		python3-boto3 \
		python3-lxml

test: copyright

.PHONY: copyright
copyright:
	$(eval MISSING_COPYRIGHT := $(shell git ls-files "*.py" | xargs grep -EL "Copyright \(c\) 20.* Aiven|Aiven license OK"))
	@if [ "$(MISSING_COPYRIGHT)" != "" ]; then echo Missing Copyright statement in files: $(MISSING_COPYRIGHT) ; false; fi


unittest: $(generated)
	$(PYTHON) -m pytest $(PYTEST_ARG) test/

coverage: $(generated)
	$(PYTHON) -m coverage run --source rpm_s3_mirror -m pytest $(PYTEST_ARG) test/
	$(PYTHON) -m coverage report --show-missing

pylint: $(generated)
	$(PYTHON) -m pylint.lint --rcfile .pylintrc $(PYTHON_SOURCE_DIRS)

flake8: $(generated)
	$(PYTHON) -m flake8 --exclude=__init__.py --ignore=E722 --max-line-len=125 $(PYTHON_SOURCE_DIRS)

.PHONY: rpm
