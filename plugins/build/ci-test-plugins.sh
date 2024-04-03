echo "================"
echo "CI TEST: PLUGINS"
echo "================"


code=0
if [ "$1" == "" ] || [ "aws/s3" = "$1" ] ; then
# aws/s3
export PYTHONPATH=plugins/aws/s3/src && python3 -m pytest -v --cov-fail-under=90 --cov-report=term --cov=plugins/aws/s3/src/ plugins/aws/s3/test/
code+=$?
fi

if [ $code -gt 0 ]
then
  echo "[FAILED] CI TEST: PLUGINS"
  exit 1
fi
echo "========================================================================================================"
exit $code
