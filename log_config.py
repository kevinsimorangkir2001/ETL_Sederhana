import logging
import os
from datetime import datetime

# function digunakan untuk mencatat seluruh kegiatan ETL meliputi mulai sampa akhir baik berhasil dan tidak
def setup_logger(name: str):
    log_dir = "log"
    os.makedirs(log_dir, exist_ok=True) #membuat folder jika tidak ditemukan/belum ada

    date_str = datetime.now().strftime("%Y-%m-%d") #tanggal hari ini
    log_path = os.path.join(log_dir, f"log_{date_str}.log") 

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler(log_path)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
