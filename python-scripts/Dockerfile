# Sử dụng image Python chính thức làm base image
FROM python:3.10-slim

# Cài đặt OpenJDK-17 thay vì OpenJDK-8
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Cài đặt các chứng chỉ cần thiết
RUN apt-get update && \
    apt-get install -y ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

# Thiết lập JAVA_HOME cho JDK 17
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
ENV PATH $JAVA_HOME/bin:$PATH

# Cài đặt các thư viện Python cần thiết
RUN pip install --no-cache-dir \
    hdfs \
    kafka-python \
    pyspark \
    elasticsearch \
    requests \
    pandas

    
EXPOSE 9093
# Sao chép mã nguồn vào container
COPY . /app

# Đặt thư mục làm việc
WORKDIR /app

# Khởi chạy ứng dụng mặc định khi container chạy

