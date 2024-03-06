SRC = $(wildcard src/*.py)

.PHONY: dev check test install clean schemas dist-only

deps: 
	cd plugins/aws/s3 && pip install -U -r requirements.txt

dev-deps: deps
	pip install -U -r requirements-dev.txt

install:
	cd plugins/aws/s3 && \
	pip install --force-reinstall -r requirements.txt && \
	pip install -U -e . --no-deps

install-examples:
	make install && \
	pip install -U -e apps/examples/aws-example/     

update-examples-api: install-examples
	bash apps/examples/aws-example/api/create_openapi_file.sh