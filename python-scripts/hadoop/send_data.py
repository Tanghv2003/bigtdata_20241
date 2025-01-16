from hdfs import InsecureClient
import os

# Khởi tạo client HDFS sử dụng WebHDFS và HTTP
client = InsecureClient('http://namenode:9870', user='root')

# Đường dẫn file trên hệ thống local và trên HDFS
local_file_path = '../dl/data.csv'  # Thay thế với đường dẫn file trên hệ thống local
hdfs_file_path = '/upload/fl_data.csv'           # Đường dẫn nơi bạn muốn upload lên HDFS

# Kiểm tra xem file có tồn tại trên hệ thống local không
if os.path.exists(local_file_path):
    try:
        # Đẩy file từ hệ thống local lên HDFS
        client.upload(hdfs_file_path, local_file_path, overwrite=True)
        print(f"File đã được upload lên HDFS tại {hdfs_file_path}")
    except Exception as e:
        print(f"Xảy ra lỗi khi đẩy file lên HDFS: {e}")
else:
    print(f"File {local_file_path} không tồn tại trên hệ thống local.")
