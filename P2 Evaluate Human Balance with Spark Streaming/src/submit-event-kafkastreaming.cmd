docker exec -it p2evaluatehumanbalancewithsparkstreaming_spark_1 /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 /home/workspace/src/sparkpyeventskafkastreamtoconsole.py | tee logs/eventstream.log 