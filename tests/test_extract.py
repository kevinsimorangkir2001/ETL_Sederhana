import unittest
from unittest.mock import patch, MagicMock
from utils.extracts import scraping_data #memanggil function scraping_data dari folder utils di scripts extracts
import requests

class TestExtract(unittest.TestCase): 
    @patch('utils.extracts.requests.get')
    def test_scraping_data_success(self, mock_get):
        # Arrange
        url = "https://fashion-studio.dicoding.dev/"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = (
        "<html><body>"
        "<div class='collection-card'>"
        "<h3 class='product-title'>Test Product</h3>"
        "<div class='price-container'>$10</div>"
        "<p>Rating: ⭐ Invalid Rating / 5</p>"
        "<p>Colors: 3 colors</p>"
        "<p>Size: M, L</p>"
        "<p>Gender: Unisex</p>"
        "</div></body></html>"
        )
        mock_get.return_value = mock_response
        
        # Act
        result = scraping_data(url)
        
        # Assert
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        self.assertIn('title', result[0])
        self.assertEqual(result[0]['title'], 'Test Product')
    
    @patch('utils.extracts.requests.get')
    def test_scrape_main_failure(self, mock_get):
        # Arrange
        url = "https://fashion-studio.dicoding.dev/"
        mock_get.side_effect = requests.exceptions.RequestException("Connection error")
        
        # Act & Assert
        with self.assertRaises(Exception) as context:
            scraping_data(url)
        self.assertIn('Tidak bisa mengambil data dari', str(context.exception))
    
    @patch('utils.extracts.requests.get')
    def test_scrape_main_parsing_error(self, mock_get):
        # Arrange
        url = "https://fashion-studio.dicoding.dev/"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>Invalid HTML not found product cards</body></html>"
        mock_get.return_value = mock_response
        
        # Act & Assert
        with self.assertRaises(Exception) as context:
            scraping_data(url)
        self.assertIn('Gagal melakukan parsing HTML', str(context.exception))

if __name__ == '__main__':  # pragma: no cover
    unittest.main()
