echo "============="
echo "CI TEST: APPS"
echo "============="
code=0

echo "apps/aws-example"
export PYTHONPATH=plugins/aws/s3/src:apps/examples/aws-example/src/ && python3 -m pytest -v --cov-fail-under=90 --cov-report=term --cov=apps/examples/aws-example/src/ apps/examples/aws-example/test/integration/
code+=$?

if [ $code -gt 0 ]
then
  echo "[FAILED] CI TEST: APPS"
fi
echo "========================================================================================================"
exit $code
