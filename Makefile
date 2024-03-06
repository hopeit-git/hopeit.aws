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
