#!/bin/bash
export OBJECT_STORAGE_ACCESS_KEY_ID="hopeit"
export OBJECT_STORAGE_SECRET_ACCESS_KEY="Hopei#Engine#2020"
export OBJECT_STORAGE_ENDPOINT_URL="http://localhost:9000"
export OBJECT_STORAGE_SSL="false"
export HOPEIT_AWS_API_VERSION=$(grep "version = \"" plugins/aws/s3/setup.py | awk -F '"' '{print $2}' | awk -F '.' '{print $1"."$2}')

export PYTHONPATH=./apps/examples/aws-example/src && \
hopeit_openapi create \
--title="AWS Example" \
--description="AWS Example" \
--api-version=$HOPEIT_AWS_API_VERSION \
--config-files=apps/examples/aws-example/config/dev-local.json,apps/examples/aws-example/config/app-config.json \
--output-file=apps/examples/aws-example/api/openapi.json
