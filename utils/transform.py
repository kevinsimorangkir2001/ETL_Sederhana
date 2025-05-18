import pandas as pd
import re
import numpy as np
from datetime import datetime
import warnings
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from log_config import setup_logger
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.set_option('future.no_silent_downcasting', True)



def transform_data(products):
    #inisiasi logging untuk mendokumentasi transform data dan tanggal
    logger = setup_logger(__name__)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info("Transformasi data dimulai pada %s", timestamp)
    EXCHANGE_RATE = 16000  # Nilai tukar USD ke IDR


    try:
        # Data Produknya dibuat dari DataFrame
        df = pd.DataFrame(products)


        # menangani invalid (seperti "Unknown Product", "Price Unavailable"), hapus baris
        initial_len = len(df)
        df = df[df['title'].str.lower() != 'unknown product']
        df = df[~df['price'].str.contains('price unavailable', na=False)]
        df = df[~df['rating'].str.contains('Invalid Rating', na=False)]
        logger.info("Menghapus %d baris dengan title tidak valid", initial_len - len(df))


        # Konversi price dari string ke float
        df['price'] = df['price'].replace(r'[^\d.]', '', regex=True)
        df['price'] = df['price'].replace('', np.nan)
        before_price = len(df)
        df.dropna(subset=['price'], inplace=True)
        after_price = len(df)
        # Konversi harga menjadi float dan x dengan nilai tukar
        df['price'] = df['price'].astype(float) * EXCHANGE_RATE
        logger.info("Kolom 'price' berhasil diproses hingga koversi ke rupiah dan menghapus %d baris data",before_price-after_price)


        # Konversi rating
        # Mengubah Rating menjadi tipe data float dan menangani format yang tidak valid
        df["rating"] = df["rating"].apply(
            lambda x: float(re.search(r"(\d+(\.\d+)?)", str(x)).group()) 
            if re.search(r"(\d+(\.\d+)?)", str(x)) else None)
        df['rating'] = df['rating'].replace('', np.nan)
        df.dropna(subset=['rating'], inplace=True)
        # Konversi rating menjadi float
        df['rating'] = df['rating'].astype(float)
        logger.info("Kolom 'rating' berhasil diambil hanya angkanya saja sebanyak %d data",len(df))


        # Bersihkan colors (hanya angka yang dipertahankan)
        df['colors'] = df['colors'].replace(r'\D', '', regex=True)
        df['colors'] = df['colors'].replace('', np.nan)
        df.dropna(subset=['colors'], inplace=True) 
        # Konversi colors menjadi integer
        df['colors'] = df['colors'].astype(int)
        logger.info("Kolom 'colors' berhasil diambil hanya angkanya saja sebanyak %d data",len(df))


        # Bersihkan size dan gender
        df['size'] = df['size'].replace(r'Size:\s*', '', regex=True)
        df['gender'] = df['gender'].replace(r'Gender:\s*', '', regex=True)
        logger.info("Kolom 'size' dan 'gender' dibersihkan")


        # Drop duplicates
        before_duplicate = len(df)
        df.drop_duplicates(inplace=True)
        after_duplicate = len(df)
        logger.info("Menghapus %d duplikat", before_duplicate - after_duplicate)


        # Drop Null
        before_null = len(df)
        df.dropna(inplace=True)
        after_null = len(df)
        logger.info("Menghapus %d null", before_null - after_null)


        # Tambahkan kolom timestamp
        df['timestamp'] = timestamp
        logger.info("Transformasi selesai, total baris akhir: %d", len(df))
        return df


    except Exception as e:
        logger.error("Terjadi kesalahan selama transformasi data: %s", str(e))
        raise Exception(f"Transformasi gagal: {str(e)}")