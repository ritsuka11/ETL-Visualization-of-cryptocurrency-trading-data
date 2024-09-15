# Hướng dẫn chạy chương trình

Yêu cầu cần cài đặt Docker trên máy tính

1. Khởi động docker daemon
2. Chạy lệnh sau để khởi động toàn bộ docker container 
```c
docker compose up -d --build
```

3. Truy cập localhost:8080, đăng nhập vào airflow với tài khoản là airflow, mật khẩu là airflow
4. Trong giao diện web của airflow kích hoạt hệ thống lập lịch tự động thu thập dữ liệu "crypto_data_pipeline".
5. Truy cập vào container spark-master
```c
docker exec -it crypto_1-spark-master-1 bash
```
6. Mở thư mục chạy chương trình
```c
cd /opt/spark-apps
```
8. Cài đặt các thư viện yêu cầu
```c
pip install -r requirements.txt
```
10. Chạy spark-submit để khởi động hệ thống xử lý dữ liệu
```c
/opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.kafka:kafka-clients:3.7.0,org.apache.commons:commons-pool2:2.12.0,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-streaming-kafka-0-10-assembly_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.driver.extraJavaOptions="-Djava.net.preferIPv4Stack=true",spark.executor.extraJavaOptions="-Djava.net.preferIPv4Stack=true" --master spark://spark-master:7077 /opt/spark-apps/spark-streaming.py

```
7. Truy cập localhost:5000 để mở giao diện trực quan.


# Ngoài ra, sau khi khởi động có thể chạy các lệnh để kiểm tra hệ thống

1. Kiểm tra dữ liệu đã được gửi đến Kafka hay chưa
```c
docker exec crypto_1-broker-1-1 kafka-console-consumer --bootstrap-server localhost:29092 --topic cryptoAllData --from-beginning
```

2. Kiểm tra dữ liệu trong cassandra
- Truy cập vào container cassandra
```c
docker-compose exec cassandra_db cqlsh
```
- Truy cập keyspace crypto_analysis
```c
USE crypto_analysis;
```
- Kiểm tra các bảng
```c
DESCRIBE TABLES;
```
- Truy xuất dữ liệu từ các bảng, ví dụ bảng coins:
```c
Select * from coins;
```
