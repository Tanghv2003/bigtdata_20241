from hdfs import InsecureClient

# Khởi tạo client HDFS sử dụng WebHDFS và HTTP
client = InsecureClient('http://localhost:9870', user='root')

# Đường dẫn thư mục cần xóa trên HDFS
folder_path = '/hadoop'

# Kiểm tra và xóa thư mục
try:
    # Tham số recursive=True để xóa toàn bộ thư mục và các file bên trong
    client.delete(folder_path, recursive=True)
    print(f"Thư mục {folder_path} đã được xóa thành công.")
except Exception as e:
    print(f"Xảy ra lỗi khi xóa thư mục: {e}")
