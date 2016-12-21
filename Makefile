.PHONY: init freeze test test_drive

export PYTHONPATH := $(realpath .)
PIP = /usr/bin/pip3
PYTEST = /usr/local/bin/py.test
PYTHON = /usr/bin/python3

init:
	$(PIP) install -r requirements.txt

freeze:
	$(PIP) freeze > requirements.txt
	echo pip install git+https://github.com/hcarvalhoalves/python-pmap.git >> requirement.txt

test:
	export PYTHONPATH=$(realpath .)
	$(PYTEST) tests

test_drive:
	export PYTHONPATH=$(realpath .)
	$(PYTHON) sql_exec_module.py TWM
