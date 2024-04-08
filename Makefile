SRC = $(wildcard src/*.py)

.PHONY: deps dev-deps clean-plugins install install-examples update-examples-api test-plugins check-plugins test-apps check-apps check test qa dist-plugin pypi-plugin pypi-test-plugin

deps:
	cd plugins/aws/s3 && pip install -U -r requirements.txt

dev-deps: deps
	pip install -U -r apps/examples/aws-example/requirements-dev.txt && \
	pip install -U -r requirements-dev.txt

clean-plugins:
	cd plugins/aws/s3 && \
	rm -rf dist build

install:
	cd plugins/aws/s3 && \
	pip install --force-reinstall -r requirements.txt && \
	pip install -U -e . --no-deps

install-examples:
	make install && \
	pip install -U -e apps/examples/aws-example/     

update-examples-api: install-examples
	bash apps/examples/aws-example/api/create_openapi_file.sh

test-plugins:
	/bin/bash plugins/build/ci-test-plugins.sh $(PLUGINFOLDER)

check-plugins:
	/bin/bash plugins/build/ci-static-plugins.sh $(PLUGINFOLDER)

test-apps:
	/bin/bash apps/build/ci-test-apps.sh
	
check-apps:
	/bin/bash apps/build/ci-static-apps.sh $(PLUGINFOLDER)

check: check-plugins check-apps

test: test-plugins test-apps

qa: test check
	echo "DONE."

dist-plugin:
	cd plugins/aws/s3/ && \
	pip install build && \
	pwd && \
	python -m build

pypi-plugin:
	pip install twine && \
	python -m twine upload -u=__token__ -p=$(PYPI_API_TOKEN_AWS) --repository pypi $(PLUGINFOLDER)/dist/*

pypi-test-plugin:
	pip install twine && \
	python -m twine upload -u=__token__ -p=$(TEST_PYPI_API_TOKEN_AWS) --repository testpypi $(PLUGINFOLDER)/dist/*
