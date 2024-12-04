# Sử dụng Spark image từ Bitnami
FROM docker.io/bitnami/spark:3.3.2

# Chuyển sang quyền root để cài đặt thêm thư viện
USER root

# Cài đặt các công cụ cần thiết
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Định nghĩa thư mục JAR Spark
ENV SPARK_JARS_DIR=/opt/bitnami/spark/jars

# Tải và cài đặt các JAR cần thiết
RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    && mv s3-2.18.41.jar $SPARK_JARS_DIR \
    && mv aws-java-sdk-1.12.367.jar $SPARK_JARS_DIR \
    && mv delta-core_2.12-2.3.0.jar $SPARK_JARS_DIR \
    && mv delta-storage-2.3.0.jar $SPARK_JARS_DIR \
    && mv mysql-connector-java-8.0.19.jar $SPARK_JARS_DIR \
    && mv hadoop-aws-3.3.2.jar $SPARK_JARS_DIR \
    && rm -f *.jar

# Đặt quyền user spark
USER 1001
