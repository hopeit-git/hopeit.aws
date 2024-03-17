echo "========================"
echo "CI STATIC ANALYSIS: APPS"
echo "========================"

declare -i code=0
echo "apps/simple-example"
export MYPYPATH=engine/src/:plugins/aws/s3/src:apps/examples/aws-example/src/ && python3 -m mypy  --check-untyped-defs  --namespace-packages -p common -p model -p aws_example 
code+=$?
# export MYPYPATH=engine/src/:plugins/aws/s3/src:apps/examples/aws-example/src/ && python3 -m mypy --namespace-packages apps/examples/aws-example/test/unit/
# code+=$?
# export MYPYPATH=engine/src/:plugins/aws/s3/src:apps/examples/aws-example/src/ && python3 -m mypy --namespace-packages apps/examples/aws-example/test/integration/
# code+=$?
python3 -m flake8 --max-line-length=120 apps/examples/aws-example/src/
code+=$?
python3 -m pylint apps/examples/aws-example/src/
code+=$?

if [ $code -gt 0 ]
then
  echo "[FAILED] CI STATIC ANALYSIS"
fi
echo "========================================================================================================"
exit $code
