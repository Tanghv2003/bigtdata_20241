import os
from hdfs import InsecureClient

class HadoopFileUploader:
    def __init__(self, 
                 hdfs_url='http://namenode:9870', 
                 hdfs_user='root', 
                 hdfs_upload_path='/hadoop/uploads/'):
        """
        Khởi tạo client HDFS
        
        :param hdfs_url: URL HDFS
        :param hdfs_user: Tên người dùng HDFS
        :param hdfs_upload_path: Đường dẫn upload trên HDFS
        """
        # Khởi tạo HDFS Client
        self.hdfs_client = InsecureClient(hdfs_url, user=hdfs_user)
        
        self.hdfs_upload_path = hdfs_upload_path

    def split_large_file(self, file_path, chunk_size=10*1024*1024):  # Mặc định 10MB
        """
        Chia file lớn thành các chunk nhỏ
        
        :param file_path: Đường dẫn file gốc
        :param chunk_size: Kích thước chunk (bytes)
        :return: Danh sách các chunk
        """
        chunks = []
        with open(file_path, 'rb') as f:
            chunk_number = 0
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                chunk_filename = f"{file_path}_chunk_{chunk_number}"
                with open(chunk_filename, 'wb') as chunk_file:
                    chunk_file.write(chunk)
                chunks.append({
                    'filename': chunk_filename,
                    'chunk_number': chunk_number,
                    'total_size': os.path.getsize(chunk_filename)
                })
                chunk_number += 1
        return chunks

    def upload_to_hdfs(self, local_file_path, hdfs_file_path):
        """
        Upload file lên HDFS
        
        :param local_file_path: Đường dẫn file local
        :param hdfs_file_path: Đường dẫn file trên HDFS
        :return: Trạng thái upload
        """
        try:
            self.hdfs_client.upload(hdfs_file_path, local_file_path)
            print(f"Đã tải {local_file_path} lên HDFS tại {hdfs_file_path}")
            return True
        except Exception as e:
            print(f"Lỗi upload HDFS: {e}")
            return False

    def send_file_to_hdfs(self, file_path):
        """
        Chia file thành các chunk và upload từng phần lên HDFS
        
        :param file_path: Đường dẫn file cần gửi
        """
        try:
            # Chia file thành các chunk
            file_chunks = self.split_large_file(file_path)
            
            # Upload từng chunk lên HDFS
            for chunk in file_chunks:
                hdfs_chunk_path = os.path.join(
                    self.hdfs_upload_path, 
                    f"{os.path.basename(file_path)}_chunk_{chunk['chunk_number']}"
                )
                
                # Upload chunk lên HDFS
                upload_status = self.upload_to_hdfs(chunk['filename'], hdfs_chunk_path)
                
            print(f"Hoàn tất upload file {file_path}")
        
        except Exception as e:
            print(f"Lỗi xử lý file: {e}")

def main():
    # Khởi tạo uploader
    uploader = HadoopFileUploader(
        hdfs_url='http://namenode:9870'
    )
    
    # Sử dụng
    large_file_path = '../../airline.csv'
    uploader.send_file_to_hdfs(large_file_path)

if __name__ == "__main__":
    main()
