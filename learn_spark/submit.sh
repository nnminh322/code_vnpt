# ./app/submit.sh
set -euo pipefail
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.host=spark-client \
  --executor-cores 1 --executor-memory 400m \
  --driver-memory 512m \
  /opt/app/count_spark.py "${N:-1000000}"
