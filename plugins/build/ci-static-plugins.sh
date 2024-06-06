echo "==========================="
echo "CI STATIC ANALYSIS: PLUGINS"
echo "==========================="

declare -i code=0

if [ "$1" == "" ] || [ "aws/s3" = "$1" ] ; then
echo "aws/s3"
isort plugins/aws/s3/src/
black plugins/aws/s3/src/
isort plugins/aws/s3/test/
black plugins/aws/s3/test/
export MYPYPATH=plugins/aws/s3/src && python3 -m mypy  --install-types --non-interactive --namespace-packages -p hopeit.aws.s3
export MYPYPATH=plugins/aws/s3/src && python3 -m mypy --namespace-packages -p hopeit.aws.s3
code+=$?
export MYPYPATH=plugins/aws/s3/src && python3 -m mypy --namespace-packages plugins/aws/s3/test/unit
code+=$?
python3 -m flake8 plugins/aws/s3/src/hopeit/aws/s3 plugins/aws/s3/test/unit
code+=$?
PYTHONPATH=plugins/aws/s3/src/ python3 -m pylint --recursive=y plugins/aws/s3/src/ plugins/aws/s3/test/unit/
code+=$?
fi

if [ $code -gt 0 ]
then
  echo "[FAILED] CI STATIC ANALYSIS: PLUGINS"
  exit 1
fi
echo "========================================================================================================"
exit $code
