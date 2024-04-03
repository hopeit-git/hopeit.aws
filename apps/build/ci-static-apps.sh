echo "========================"
echo "CI STATIC ANALYSIS: APPS"
echo "========================"

declare -i code=0
echo "apps/aws-example"
isort apps/examples/aws-example/src/
black apps/examples/aws-example/src/
isort apps/examples/aws-example/test/
black apps/examples/aws-example/test/
export MYPYPATH=plugins/aws/s3/src:apps/examples/aws-example/src/ && python3 -m mypy  --install-types  --check-untyped-defs --non-interactive --namespace-packages -p  aws_example
export MYPYPATH=plugins/aws/s3/src:apps/examples/aws-example/src/ && python3 -m mypy  --check-untyped-defs  --namespace-packages -p  aws_example
code+=$?
python3 -m flake8 apps/examples/aws-example/src/
code+=$?
python3 -m pylint apps/examples/aws-example/src/
code+=$?

if [ $code -gt 0 ]
then
  echo "[FAILED] CI STATIC ANALYSIS"
fi
echo "========================================================================================================"
exit $code
