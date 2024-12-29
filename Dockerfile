# Sử dụng Spark image từ Bitnami
FROM docker.io/bitnami/spark:latest

# Chuyển sang quyền root để cài đặt thêm thư viện
USER root

# Cài đặt các công cụ cần thiết và dọn dẹp
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Định nghĩa thư mục JAR Spark
ENV SPARK_JARS_DIR=/opt/bitnami/spark/jars

# Tải và cài đặt các JAR cần thiết vào thư mục JAR của Spark
RUN echo "Downloading Spark JAR files..." && \
    curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar && \
    curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar && \
    curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar && \
    curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar && \
    # Kafka Dependencies
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.0/spark-token-provider-kafka-0-10_2.12-3.4.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.4.0/spark-streaming-kafka-0-10_2.12-3.4.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka_2.12/3.4.0/kafka_2.12-3.4.0.jar && \
    # Move all JAR files
    echo "Moving JAR files to Spark JAR directory..." && \
    mv s3-2.18.41.jar $SPARK_JARS_DIR && \
    mv aws-java-sdk-1.12.367.jar $SPARK_JARS_DIR && \
    mv delta-core_2.12-2.3.0.jar $SPARK_JARS_DIR && \
    mv delta-storage-2.3.0.jar $SPARK_JARS_DIR && \
    mv mysql-connector-java-8.0.19.jar $SPARK_JARS_DIR && \
    mv hadoop-aws-3.3.2.jar $SPARK_JARS_DIR && \
    mv spark-sql-kafka-0-10_2.12-3.4.0.jar $SPARK_JARS_DIR && \
    mv spark-token-provider-kafka-0-10_2.12-3.4.0.jar $SPARK_JARS_DIR && \
    mv kafka-clients-3.4.0.jar $SPARK_JARS_DIR && \
    mv commons-pool2-2.11.1.jar $SPARK_JARS_DIR && \
    mv spark-streaming-kafka-0-10_2.12-3.4.0.jar $SPARK_JARS_DIR && \
    mv kafka_2.12-3.4.0.jar $SPARK_JARS_DIR && \
    rm -f *.jar

# Kiểm tra sự tồn tại của tệp spark-class
RUN ls -l /opt/bitnami/spark/bin/

# Chuyển lại quyền cho người dùng Spark (user 1001)
USER 1001