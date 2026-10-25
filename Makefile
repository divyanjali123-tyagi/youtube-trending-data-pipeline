.PHONY: install test lint local deploy run destroy

install:
	pip install -r requirements-dev.txt

test:
	pytest

lint:
	ruff check .
	cfn-lint infrastructure/template.yaml

local:
	python scripts/run_local.py

deploy:
	python scripts/pipeline_ctl.py deploy

run:
	python scripts/pipeline_ctl.py run

destroy:
	python scripts/pipeline_ctl.py destroy
