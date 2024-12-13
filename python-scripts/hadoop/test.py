from hdfs import InsecureClient
import os

# Khởi tạo client HDFS sử dụng WebHDFS và HTTP
client = InsecureClient('http://namenode:9870', user='root')

# Đường dẫn file trên HDFS nơi bạn muốn tải lên
hdfs_file_path = '/test/hello.txt'

# Dữ liệu bạn muốn ghi vào file
data = "xin chào"

# Kiểm tra nếu file không tồn tại thì tạo mới và ghi dữ liệu vào HDFS
try:
    # Kiểm tra xem file có tồn tại trên HDFS không
    if not client.status(hdfs_file_path, strict=False):
        print(f"File {hdfs_file_path} chưa tồn tại. Đang tạo mới...")

    # Gửi dữ liệu lên HDFS (mở kết nối ghi file)
    with client.write(hdfs_file_path, overwrite=True) as writer:
        writer.write(data.encode())  # Ghi dữ liệu vào file trên HDFS (encode để chuyển đổi thành byte)

    print(f"Dữ liệu đã được gửi lên HDFS tại {hdfs_file_path}")
except Exception as e:
    print(f"Xảy ra lỗi khi gửi dữ liệu lên HDFS: {e}")
