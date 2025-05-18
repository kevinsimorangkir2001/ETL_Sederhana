import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from utils.load import save_to_csv, save_to_postgresql, save_to_google_sheets

class TestLoad(unittest.TestCase):
    @patch("utils.load.pd.DataFrame.to_csv")
    def test_save_to_csv(self, mock_to_csv):
        """Test untuk memastikan data disimpan ke CSV"""
        df = pd.DataFrame({'Title': ['Pullbear Hoodie'], 'Price': [123.45]})
        save_to_csv(df, 'test_products.csv')
        mock_to_csv.assert_called_once_with('test_products.csv', index=False)


    @patch("utils.load.pd.DataFrame.to_csv", side_effect=Exception("Write error"))
    def test_save_to_csv_failure(self, mock_to_csv):
        df = pd.DataFrame({'Title': ['Error Case'], 'Price': [40000]})
        save_to_csv(df, 'fail.csv')


    @patch("utils.load.psycopg2.connect")
    def test_save_to_postgresql_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        df = pd.DataFrame({
            'title': ['T-shirt'],
            'price': [123.45],
            'rating': [4.0],
            'colors': [2],
            'size': ['M'],
            'gender': ['Men'],
            'timestamp': ['2025-05-18 12:00:00']
        })
        db_config = {"host": "localhost", "database": "test_db", "user": "user", "password": "password", "port": "5432"}
        save_to_postgresql(df, db_config, "test_table")

        mock_connect.assert_called_once_with(**db_config)
        mock_conn.cursor.assert_called_once()
        self.assertTrue(mock_cursor.execute.called)
        self.assertTrue(mock_conn.close.called)

    @patch("utils.load.psycopg2.connect", side_effect=Exception("Connection error"))
    def test_save_to_postgresql_failure(self, mock_connect):
        df = pd.DataFrame({
            'title': ['Error Case'],
            'price': [123],
            'rating': [4.0],
            'colors': [2],
            'size': ['M'],
            'gender': ['Men'],
            'timestamp': ['2025-05-18 12:00:00']
        })
        db_config = {"host": "localhost", "database": "test_db", "user": "user", "password": "password", "port": "5432"}
        save_to_postgresql(df, db_config, "fail_table")


    @patch("utils.load.build")
    def test_save_to_google_sheets(self, mock_build):
        """Test untuk memastikan data disimpan ke Google Sheets"""
        df = pd.DataFrame({'Title': ['Hoodie jeans'], 'Price': [36000]})
        save_to_google_sheets(df, 'spreadsheet_id', 'fashion!A1')        
        mock_build.assert_called_once()


    @patch("utils.load.Credentials.from_service_account_file")
    @patch("utils.load.build")
    def test_save_to_google_sheets_success(self, mock_build, mock_credentials):
        mock_service = MagicMock()
        mock_sheets = MagicMock()
        mock_service.spreadsheets.return_value = mock_sheets
        mock_build.return_value = mock_service

        df = pd.DataFrame({
            'Title': ['Hoodie jeans'],
            'Price': [36000]
        })
        save_to_google_sheets(df, 'spreadsheet_id', 'fashion!A1')

        mock_build.assert_called_once()


    def test_save_to_google_sheets_empty_df(self):
        df = pd.DataFrame()
        save_to_google_sheets(df, 'spreadsheet_id', 'Sheet1!A1')


    @patch("utils.load.Credentials.from_service_account_file", side_effect=FileNotFoundError())
    @patch("utils.load.build")
    def test_save_to_google_sheets_file_not_found(self, mock_build, mock_creds):
        df = pd.DataFrame({'Title': ['T-shirt'], 'Price': [10000]})
        save_to_google_sheets(df, 'spreadsheet_id', 'Sheet1!A1')


    @patch("utils.load.Credentials.from_service_account_file")
    @patch("utils.load.build", side_effect=Exception("Upload error"))
    def test_save_to_google_sheets_general_exception(self, mock_build, mock_creds):
        df = pd.DataFrame({'Title': ['T-shirt'], 'Price': [10000]})
        save_to_google_sheets(df, 'spreadsheet_id', 'Sheet1!A1')


if __name__ == '__main__':  # pragma: no cover
    unittest.main()