from hdfs import InsecureClient

# Cấu hình kết nối HDFS
hdfs_client = InsecureClient('http://namenode:9870', user='root')

# Dữ liệu cần gửi lên HDFS (ví dụ: dòng chữ "Hello")
data = "Hello this is client hadoop"

# Đường dẫn đích trên HDFS
hdfs_file_path = '/tanghv/test.txt'

# Ghi dữ liệu lên HDFS
with hdfs_client.write(hdfs_file_path, overwrite=True) as writer:
    writer.write(data)
    print(f'Data "Hello" successfully uploaded to {hdfs_file_path}')
