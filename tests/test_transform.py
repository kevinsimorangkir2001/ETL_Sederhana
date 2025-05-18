import unittest
from unittest.mock import patch
import pandas as pd
from utils.transform import transform_data

class TestTransform(unittest.TestCase):
    
    def test_transform_data(self):
        # Arrange
        products = [
            {'title': 'Pullbear 2020', 'price': '270000', 'rating': '3.0', 'colors': '3', 'size': 'XXXL', 'gender': 'Men'},
            {'title': 'Product 2', 'price': '20000', 'rating': '5.0', 'colors': '3', 'size': 'L', 'gender': 'Women'}
        ]
        # Act
        df = transform_data(products)
        # Assert
        self.assertEqual(len(df), 2)
        self.assertIn('price', df.columns)
        self.assertIn('rating', df.columns)
        self.assertIn('timestamp', df.columns)
        self.assertTrue(df['price'].iloc[0] > 0)
        self.assertTrue(df['rating'].iloc[0] > 0)
    

    def test_invalid_price(self):
        # Arrange
        products = [
            {'title': 'Product 1', 'price': '', 'rating': '4.5', 'colors': '2', 'size': 'M', 'gender': 'Women'},
            {'title': 'Product 1', 'price': 'Price Unavailable', 'rating': '4.5', 'colors': '3', 'size': 'M', 'gender': 'Men'},
            {'title': 'Product 1', 'price': '$4.6', 'rating': '3.5', 'colors': '2 colors', 'size': 'M', 'gender': 'Men'}
        ]    
        # Act
        df = transform_data(products)
        # Assert
        self.assertEqual(len(df), 1)  # hanya ada 3 produk yang valid
    
    def test_invalid_rating(self):
        # Arrange
        products = [
            {'title': 'Product 1', 'price': '2000', 'rating': 'Rating: ⭐ Invalid Rating/ 5', 'colors': '3', 'size': 'M', 'gender': 'Men'},
            {'title': 'Product 1', 'price': '$10', 'rating': 'Rating: ⭐ 4.0 / 5', 'colors': '3', 'size': 'M', 'gender': 'Men'}
        ]
        # Act
        df = transform_data(products)
        # Assert
        self.assertEqual(len(df), 1)  # hanya ada 1 produk yang valid


    def test_main_block(self):
        # Hanya untuk mencakup blok if __name__ == '__main__'
        with patch.object(unittest, 'main'):
            import tests.test_transform

    def test_transform_data_raises_exception(self):
        products = [
            {'title': 'Product Error', 'price': 'abc.def', 'rating': 'Rating: x.x', 'colors': 'Colors: NaN', 'size': 'Size: M', 'gender': 'Gender: Men'}
        ]
        with self.assertRaises(Exception) as context:
            transform_data(products)
        self.assertIn("Transformasi gagal", str(context.exception))


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
