import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime
import warnings
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from log_config import setup_logger
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.set_option('future.no_silent_downcasting', True)


#inisiasi logging untuk mendokumentasi load data dan tanggal
logger = setup_logger(__name__)
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def save_to_csv(df, filename='products.csv'):
    """
    Fungsi untuk menyimpan DataFrame ke dalam file CSV.
    :param df: DataFrame yang berisi data produk
    :param filename: Nama file CSV untuk menyimpan data
    """
    try:
        df.to_csv(filename, index=False)  # Menyimpan data ke dalam 
        logger.info(f"Data ETL berhasil disimpan ke {filename} pada tanggal {timestamp}")
        print(f"Data berhasil disimpan ke {filename}")
    except Exception as e:
        logger.error(f"Terjadi kesalahan saat menyimpan data ke CSV: {e} tanggal {timestamp}")
        print(f"Terjadi kesalahan saat menyimpan data ke CSV: {e}")



# Fungsi untuk menyimpan data ke Google Sheets
def save_to_google_sheets(df, spreadsheet_id, range_name, credentials_file='etl-pipeline-sederhana.json'):
    """
    Fungsi untuk mengupload DataFrame ke Google Sheets
    :param df: DataFrame yang berisi data produk
    :param spreadsheet_id: ID dari spreadsheet Google Sheets
    :param range_name: Rentang tempat data akan dimasukkan (e.g., "Sheet1!A1")
    :param credentials_file: File JSON kredensial untuk Google Sheets API
    """
    if df.empty:
        logger.error(f"Tidak ada data untuk diupload ke Google Sheets tanggal {timestamp}.")
        print("Tidak ada data untuk diupload ke Google Sheets.")
        return

    try:
        # Membaca kredensial dari file JSON
        creds = Credentials.from_service_account_file(credentials_file)
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Mengonversi DataFrame menjadi list of lists untuk Google Sheets
        values = [df.columns.tolist()] + df.values.tolist()
        body = {'values': values}

        # Update data ke Google Sheets
        sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body=body
        ).execute()
        logger.info(f"Data berhasil disimpan ke Google Sheets di {spreadsheet_id} pada tanggal {timestamp}")
        print(f"Data berhasil disimpan ke Google Sheets di {spreadsheet_id}")

    except FileNotFoundError:
        logger.error(f"gagal menyimpan ke google sheet karena File kredensial {credentials_file} tidak ditemukan pada tanggal {timestamp}.")
        print(f"File kredensial {credentials_file} tidak ditemukan.")
    except Exception as e:
        logger.error(f"Terjadi kesalahan saat mengupload data ke Google Sheets: {e} tanggal {timestamp}")
        print(f"Terjadi kesalahan saat mengupload data ke Google Sheets: {e}")


# Fungsi untuk menyimpan data ke PostgreSQL
def save_to_postgresql(df, db_config, table_name="products"):
    """
    Fungsi untuk menyimpan DataFrame ke dalam PostgreSQL.
    :param df: DataFrame yang berisi data produk
    :param db_config: Konfigurasi koneksi ke PostgreSQL
    :param table_name: Nama tabel di PostgreSQL (default 'products')
    """
    conn = None
    try:
        # Membuat koneksi ke PostgreSQL
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Membuat tabel jika belum ada dengan dibuat primary key otomatis
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            title TEXT,
            price FLOAT,
            rating FLOAT,
            colors INT,
            size TEXT,
            gender TEXT,
            timestamp TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Menyimpan data ke tabel PostgreSQL
        for _, row in df.iterrows():
            cursor.execute(
                f"INSERT INTO {table_name} (title, price, rating, colors, size, gender, timestamp) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (row['title'], row['price'], row['rating'], row['colors'], row['size'], row['gender'], row['timestamp'])
            )
        conn.commit()
        logger.info(f"Data berhasil disimpan ke PostgreSQL ke tabel {table_name} tanggal {timestamp}")
        print(f"Data berhasil disimpan ke PostgreSQL ke tabel {table_name}")
    except Exception as e:
        logger.error(f"Terjadi kesalahan saat menyimpan data ke PostgreSQL: {e} tanggal {timestamp}")
        print(f"Terjadi kesalahan saat menyimpan data ke PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()