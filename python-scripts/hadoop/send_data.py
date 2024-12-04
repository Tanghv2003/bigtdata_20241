import csv
from hdfs import InsecureClient
import os

# Cấu hình kết nối HDFS
hdfs_client = InsecureClient('http://namenode:9870', user='root')

# Đọc dữ liệu từ file CSV
csv_file_path = '../test.csv'

# Đường dẫn đích trên HDFS
hdfs_file_path = '/tanghv/test.csv'
hdfs_directory_path = os.path.dirname(hdfs_file_path)

# Kiểm tra nếu thư mục trên HDFS chưa tồn tại, tạo mới
if not hdfs_client.status(hdfs_directory_path, strict=False):  # strict=False để không gây lỗi nếu thư mục không tồn tại
    hdfs_client.makedirs(hdfs_directory_path)
    print(f'Directory {hdfs_directory_path} created on HDFS.')

# Mở file CSV và gửi từng dòng lên HDFS
with open(csv_file_path, mode='r', newline='', encoding='utf-8') as csvfile:
    csvreader = csv.reader(csvfile)
    
    # Mở file trên HDFS để ghi dữ liệu
    with hdfs_client.write(hdfs_file_path, overwrite=True) as writer:
        # Duyệt qua từng dòng trong CSV và gửi lên HDFS
        for row in csvreader:
            row_data = ','.join(row)  # Chuyển dòng thành chuỗi
            writer.write(row_data + '\n')  # Ghi vào HDFS và thêm dòng mới
            print(f'Row "{row_data}" successfully uploaded to {hdfs_file_path}')
