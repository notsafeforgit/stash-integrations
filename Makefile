UV ?= uv
PYTHON := $(CURDIR)/.venv/bin/python
export PYTHONDONTWRITEBYTECODE := 1

.PHONY: setup test test-producer test-archive test-library test-tools build
setup:
	$(UV) venv --allow-existing --python python3 .venv
	$(UV) pip install --python $(PYTHON) './integrations/gallery-dl[gallery]' ./integrations/archive ./integrations/library
	$(PYTHON) -I -B scripts/verify_producer_install.py

test: test-producer test-archive test-library test-tools

test-producer:
	PYTHONPATH=integrations/gallery-dl/src $(PYTHON) -m unittest discover -s integrations/gallery-dl/tests -v

test-archive:
	PYTHONPATH=integrations/archive/src:integrations/gallery-dl/src $(PYTHON) -m unittest discover -s integrations/archive/tests -v

test-library:
	$(PYTHON) -m unittest discover -s integrations/library -p 'test_*.py' -v

test-tools:
	$(PYTHON) -m unittest discover -s scripts -p 'test_*.py' -v

build:
	$(UV) build integrations/gallery-dl --out-dir dist
	$(UV) build integrations/archive --out-dir dist
	$(UV) build integrations/library --out-dir dist
