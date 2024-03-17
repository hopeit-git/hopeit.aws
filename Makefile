SRC = $(wildcard src/*.py)

.PHONY: dev check test install clean schemas dist-only

deps: 
	cd plugins/aws/s3 && pip install -U -r requirements.txt

dev-deps: deps
	pip install -U -r requirements-dev.txt

lock-requirements: dev-deps	
	pip freeze > requirements.lock

locked-deps:	
	pip install --force-reinstall -r requirements.lock && \
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

test-plugins:
	/bin/bash plugins/build/ci-test-plugins.sh $(PLUGINFOLDER)

check-plugins:
	/bin/bash plugins/build/ci-static-plugins.sh $(PLUGINFOLDER)

check-apps:
	/bin/bash apps/build/ci-static-apps.sh $(PLUGINFOLDER)

check: check-plugins check-apps

test: test-plugins

qa: test check
	echo "DONE."