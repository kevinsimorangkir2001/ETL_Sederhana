# ETL Pipeline Sederhana
## Submission Belajar Fundamental Pemrosesan Data (Dicoding)
Proyek ini merupakan implementasi dari dasar-dasar pemrosesan data dengan Python. Fokus utamanya adalah bagaimana cara mengambil, mengolah, dan menguji data dari berbagai sumber agar siap digunakan untuk keperluan praktis, seperti analisis data atau otomasi sederhana.

# Deskripsi singkat
Tujuan utama proyek ini adalah untuk mendapatkan pemahaman tentang bagaimana data dapat diambil, dimanipulasi, hingga disimpan didatabase menggunakan Python. Metode-metode ini termasuk teknik scraping data dari web, merapikan data mentah agar lebih terstruktur, dan menyimpan hasil transform menambahkan pengujian otomatis untuk memastikan skrip berjalan dengan benar.
# Fitur Utama
beberapa fitur utama seperti:
- `Web Scrapping` untuk mengumpulkan data secara otomatis dari situs web tertentu, dan kemudian menyusunnya dalam format yang lebih mudah diproses.
- `Transformasi Data` untuk Mengatur dan memperbaiki data agar siap untuk analisis atau disimpan dalam format yang lebih teratur.
- `Unit Testing` untuk menyediakan pengujian otomatis untuk memastikan bahwa setiap modul atau fungsi berfungsi dengan baik.
- `Menambahkan Logging` untuk mencatat seluruh kegiatan ETL meliputi mulai sampa akhir baik berhasil dan tidak
# Langkah untuk menjalan proyek ini
## membuat environment
python -m venv env
## mengaktifkan environment (windows) digunakan
source env/Scripts/activate
## Instal dependency
pip install -r requirements.txt
## Buat otomatis requirements (optional)
pip freeze > requirements.txt
## Jalanin Skrip
python main.py
## Jalanin unit test yang ada di folder tests
python -m unittest discover tests
## Jalanin test coverage yang ada di folder tests
coverage run -m unittest discover tests

## lihat hasil report
coverage report -m

# Link Google Sheets:
https://docs.google.com/spreadsheets/d/1yFUgSlNPsuiu8d8fxzPAGcxbNrxAioumBGLOD6gQX0w/edit?usp=sharing

