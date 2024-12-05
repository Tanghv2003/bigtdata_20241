import os
import logging
from config import Config

class FileSplitter:
    @staticmethod
    def split_file(input_file, chunk_size=Config.CHUNK_SIZE):
        """
        Split large file into manageable chunks
        
        Args:
            input_file (str): Path to the input file
            chunk_size (int): Size of each chunk in bytes
        
        Yields:
            tuple: (chunk_number, chunk_data)
        """
        logging.info(f"Starting to split file: {input_file}")
        
        with open(input_file, 'rb') as f:
            chunk_number = 0
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                
                chunk_number += 1
                yield chunk_number, chunk
        
        logging.info(f"File split completed. Total chunks: {chunk_number}")