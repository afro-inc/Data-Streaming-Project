docker compose up -d --build

docker cp .\mysql-connector-j_9.1.0-1debian12_all.deb airflow-spark_master-1:/usr/share/java/ 

docker cp dags/processing/spark_processing.py airflow-spark_master-1:/opt/bitnami/spark/

$command = "spark-submit --master local[1] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --jars /usr/share/java/mysql-connector-j-9.1.0.jar /opt/bitnami/spark/spark_processing.py"

docker exec airflow-spark_master-1 sh -c $command