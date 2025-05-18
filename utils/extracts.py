import requests
from bs4 import BeautifulSoup
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from log_config import setup_logger
from datetime import datetime

def scraping_data(url):
    logger = setup_logger(__name__)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  #  jika tidak sukses(200) maka handling error dengan raise exception
    except requests.exceptions.RequestException as e:
        logger.error(f"Tidak bisa mengambil data dari {url}. Detail: {e}")
        raise Exception(f"Tidak bisa mengambil data dari {url}. Detail: {e}")
    
    try:
        soup = BeautifulSoup(response.text, 'html.parser')
        products = []
        
        # Tambahkan validasi HTML parsing
        if not soup.find_all('div', class_='collection-card'):
            logger.error("Gagal melakukan parsing HTML karena tidak ditemukan")
            raise Exception("Gagal melakukan parsing HTML karena tidak ditemukan")
            
        for card in soup.find_all('div', class_='collection-card'):
            title_tag = card.find('h3', class_='product-title')
            title = title_tag.text.strip() if title_tag else 'No Title Info'
            
            price_tag = card.find('div', class_='price-container')
            price = price_tag.text.strip() if price_tag else 'No Price Info'
            
            rating_tag = card.find('p', string=lambda text: text and 'Rating' in text)
            rating = rating_tag.text.strip() if rating_tag else 'No Rating Info'
            
            colors_tag = card.find('p', string=lambda text: text and 'Colors' in text)
            colors = colors_tag.text.strip() if colors_tag else 'No Color Info'
            
            size_tag = card.find('p', string=lambda text: text and 'Size' in text)
            size = size_tag.text.strip() if size_tag else 'No Size Info'
            
            gender_tag = card.find('p', string=lambda text: text and 'Gender' in text)
            gender = gender_tag.text.strip() if gender_tag else 'No Gender Info'
            
            products.append({
                'title': title,
                'price': price,
                'rating': rating,
                'colors': colors,
                'size': size,
                'gender': gender
            })
        
        if not products:
            raise Exception("Tidak ditemukan produk")
        
        logger.info(f"Berhasil melakukan extract tanggal {timestamp}")
        return products            
    
    except Exception as e:
        logger.error(f"Gagal melakukan parsing: {str(e)} tanggal {timestamp}")
        raise Exception(f"Gagal melakukan parsing: {str(e)}")

    