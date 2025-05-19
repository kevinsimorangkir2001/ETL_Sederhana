import os
from datetime import datetime
from utils.extracts import scraping_data
from log_config import setup_logger # untuk mencatat berhasil tidaknya data diambil
from utils.transform import transform_data
from utils.load import save_to_csv, save_to_google_sheets, save_to_postgresql
from dotenv import load_dotenv

load_dotenv()

def main():
    logger = setup_logger(__name__)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    base_url = 'https://fashion-studio.dicoding.dev/'
    all_products = []


    # Halaman Pertama tanpa page
    logger.info(f"berhasil Scraping main page: {base_url}")
    print(f"Scraping main page: {base_url}")
    try:
        products = scraping_data(base_url)
        all_products.extend(products)
    except Exception as e:
        logger.error(f"Gagal melakukan Scrapping halaman 1: {e}")
        print(f"Gagal melakukan Scrapping halaman 1: {e}")


    # Halaman 2 hingga 50
    for page in range(2, 51):
        url = f"{base_url}page{page}"
        logger.info(f"Berhasil Scraping page {page}: {url}")
        print(f"Scraping page {page}: {url}")
        try:
            products = scraping_data(url)
            all_products.extend(products)
        except Exception as e:
            logger.error(f"Gagal melakukan scrapping {page}: {e}")
            print(f"Gagal melakukan scrapping {page}: {e}")


    # # Transform data
    transformed_data = transform_data(all_products)


    # Save data ke csv
    save_to_csv(transformed_data)
    
    # save data ke Postgresql
    # Konfigurasi untuk menyimpan ke PostgreSQL    
    db_config = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
       "host" : os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT")
    }
    save_to_postgresql(transformed_data,db_config)

    # Simpan ke Google Sheets
    # Mendapatkan kredensial dan spreadsheet ID 
    spreadsheet_id = "1yFUgSlNPsuiu8d8fxzPAGcxbNrxAioumBGLOD6gQX0w"
    range_name = 'fashion!A1'
    save_to_google_sheets(transformed_data, spreadsheet_id, range_name)
    logger.info(f"Proses ETL PT Dicoding selesai dilakukan tanggal {timestamp}")
    print("\nProses ETL selesai")

if __name__ == '__main__':
    main()