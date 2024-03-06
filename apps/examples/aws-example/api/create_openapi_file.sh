#!/bin/bash
export OBJECT_STORAGE_ACCESS_KEY_ID="hopeit"
export OBJECT_STORAGE_SECRET_ACCESS_KEY="Hopei#Engine#2020"
export OBJECT_STORAGE_ENDPOINT_URL="http://localhost:9000"
export OBJECT_STORAGE_SSL="false"

export PYTHONPATH=./apps/examples/aws-example/src && \
hopeit_openapi create \
--title="AWS Example" \
--description="AWS Example" \
--api-version="0.1.0" \
--config-files=apps/examples/aws-example/config/dev-local.json,apps/examples/aws-example/config/app-config.json \
--output-file=apps/examples/aws-example/api/openapi.json
