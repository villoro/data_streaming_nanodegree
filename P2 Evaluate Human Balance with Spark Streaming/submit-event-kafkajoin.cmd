docker exec -it 158ca42968dc /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /home/workspace/project/villoro/sparkpykafkajoin.py | tee logs/kafkajoin.log 