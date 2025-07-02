import unittest
from unittest.mock import patch, mock_open, MagicMock
import os
import pandas as pd
import gzip
import shutil
from datetime import datetime
import requests
import mysql.connector
from bs4 import BeautifulSoup

# Import functions and classes from wiki_crawl.py
from wiki_crawl import (
    Wiki,
    DataBase,
    format_url,
    validate_date_input,
    get_search_span,
    display_search_dates,
    write_links_to_file,
    unzip_file,
    search_file,
    delete_searched_files,
    download_and_process_file
)

# Patch the config module where it's imported in wiki_crawl.py
# This needs to be done before any imports that rely on `config`
# The actual patching of external calls will be done within test methods

class TestWikiCrawlFunctions(unittest.TestCase):

    def test_format_url(self):
        self.assertEqual(format_url('http://base.com/', '2023', '01'), 'http://base.com/2023/2023-01/')
        self.assertEqual(format_url('https://dumps.wikimedia.org/other/pageviews/', '2015', '12'), 'https://dumps.wikimedia.org/other/pageviews/2015/2015-12/')

    def test_validate_date_input_valid(self):
        year, month, start_day, end_day = validate_date_input(str(datetime.now().year), '06', '01', '30')
        self.assertEqual((year, month, start_day, end_day), (datetime.now().year, 6, 1, 30))

    def test_validate_date_input_invalid_year(self):
        with self.assertRaises(ValueError):
            validate_date_input('2010', '01', '01', '01') # Before 2015
        with self.assertRaises(ValueError):
            validate_date_input(str(datetime.now().year + 1), '01', '01', '01') # Future year

    def test_validate_date_input_invalid_month(self):
        with self.assertRaises(ValueError):
            validate_date_input('2023', '00', '01', '01')
        with self.assertRaises(ValueError):
            validate_date_input('2023', '13', '01', '01')

    def test_validate_date_input_invalid_day(self):
        with self.assertRaises(ValueError):
            validate_date_input('2023', '01', '00', '01')
        with self.assertRaises(ValueError):
            validate_date_input('2023', '01', '01', '32')
        with self.assertRaises(ValueError):
            validate_date_input('2023', '04', '01', '31') # April has 30 days

    def test_validate_date_input_start_day_after_end_day(self):
        with self.assertRaises(ValueError):
            validate_date_input('2023', '01', '10', '05')

    def test_validate_date_input_february_leap_year(self):
        year, month, start_day, end_day = validate_date_input('2024', '02', '01', '29')
        self.assertEqual((year, month, start_day, end_day), (2024, 2, 1, 29))
        with self.assertRaises(ValueError):
            validate_date_input('2024', '02', '01', '30')

    def test_validate_date_input_february_non_leap_year(self):
        year, month, start_day, end_day = validate_date_input('2023', '02', '01', '28')
        self.assertEqual((year, month, start_day, end_day), (2023, 2, 1, 28))
        with self.assertRaises(ValueError):
            validate_date_input('2023', '02', '01', '29')

    def test_get_search_span(self):
        self.assertEqual(get_search_span(2023, 1, 1, 3), ['20230101', '20230102', '20230103'])
        self.assertEqual(get_search_span(2023, 12, 25, 25), ['20231225'])

    def test_display_search_dates(self):
        self.assertEqual(display_search_dates(['20230101', '20230102']), '20230101 - 20230102')
        self.assertEqual(display_search_dates(['20230101']), '20230101')
        self.assertEqual(display_search_dates([]), 'No dates to display.')

    @patch('builtins.open', new_callable=mock_open)
    def test_write_links_to_file(self, mock_file_open):
        mock_links = [
            {'href': 'pageviews/2023/20230101-hourly.gz'},
            {'href': 'pageviews/2023/20230102-hourly.gz'},
            {'href': 'some_other_link'}
        ]
        search_dates = ['20230101', '20230102']
        write_links_to_file(mock_links, search_dates, 'test_links.txt')
        mock_file_open.assert_called_once_with('test_links.txt', 'w')
        handle = mock_file_open()
        handle.write.assert_any_call('pageviews/2023/20230101-hourly.gz\n')
        handle.write.assert_any_call('pageviews/2023/20230102-hourly.gz\n')
        self.assertEqual(handle.write.call_count, 2)

    @patch('gzip.open')
    @patch('builtins.open', new_callable=mock_open)
    @patch('shutil.copyfileobj')
    def test_unzip_file(self, mock_copyfileobj, mock_builtins_open, mock_gzip_open):
        mock_gzip_file_handle = MagicMock()
        mock_gzip_open.return_value.__enter__.return_value = mock_gzip_file_handle

        mock_output_file_handle = MagicMock()
        mock_builtins_open.return_value.__enter__.return_value = mock_output_file_handle

        result = unzip_file('test.gz')

        mock_gzip_open.assert_called_once_with('test.gz', 'rb')
        mock_builtins_open.assert_called_once_with('test.gz.txt', 'wb')
        mock_copyfileobj.assert_called_once_with(mock_gzip_file_handle, mock_output_file_handle)
        self.assertEqual(result, 'test.gz.txt')

    @patch('pandas.read_csv')
    def test_search_file(self, mock_read_csv):
        # Mock a DataFrame that pandas.read_csv would return
        mock_df = pd.DataFrame({
            'Domain Code': ['en', 'en', 'fr'],
            'Page Title': ['Python', 'Java', 'Python'],
            'Count Views': [100, 50, 200],
            'Response Size': [10, 5, 20]
        })
        mock_read_csv.return_value = mock_df

        # Test case 1: Search term found
        total_searches = search_file('dummy_file.txt', 'Python', 0)
        self.assertEqual(total_searches, 300) # 100 + 200

        # Test case 2: Search term not found
        total_searches = search_file('dummy_file.txt', 'C++', 0)
        self.assertEqual(total_searches, 0)

        # Test case 3: Existing total searches
        total_searches = search_file('dummy_file.txt', 'Java', 100)
        self.assertEqual(total_searches, 150) # 100 (initial) + 50 (Java)

    @patch('os.path.exists')
    @patch('os.remove')
    def test_delete_searched_files(self, mock_remove, mock_exists):
        # Test case 1: File exists and is deleted
        mock_exists.return_value = True
        delete_searched_files(['file1.txt', 'file2.txt'])
        self.assertEqual(mock_remove.call_count, 2)
        mock_remove.assert_any_call('file1.txt')
        mock_remove.assert_any_call('file2.txt')

        # Test case 2: File does not exist
        mock_exists.return_value = False
        mock_remove.reset_mock() # Reset mock call count
        delete_searched_files(['non_existent_file.txt'])
        mock_remove.assert_not_called()

class TestWikiClass(unittest.TestCase):

    @patch('wiki_crawl.requests.get') # Patch requests.get where it's used in wiki_crawl
    def test_fetch_page_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '<html><body><a href="test">Test</a></body></html>'
        mock_get.return_value = mock_response

        wiki = Wiki('http://example.com')
        wiki.fetch_page()

        mock_get.assert_called_once_with('http://example.com', timeout=10)
        self.assertIsNotNone(wiki.soup)
        self.assertEqual(wiki.soup.find('a').text, 'Test')

    @patch('wiki_crawl.requests.get') # Patch requests.get where it's used in wiki_crawl
    def test_fetch_page_failure(self, mock_get):
        mock_get.side_effect = requests.exceptions.RequestException('Network error')

        wiki = Wiki('http://example.com')
        with self.assertRaises(requests.exceptions.RequestException):
            wiki.fetch_page()
        self.assertIsNone(wiki.soup)

    def test_find_all_links(self):
        wiki = Wiki('http://example.com')
        wiki.soup = BeautifulSoup('<html><body><a href="link1">Link 1</a><a href="link2">Link 2</a></body></html>', 'html.parser')
        links = wiki.find_all_links()
        self.assertEqual(len(links), 2)
        self.assertEqual(links[0]['href'], 'link1')

    def test_find_all_links_no_soup(self):
        wiki = Wiki('http://example.com')
        links = wiki.find_all_links()
        self.assertEqual(len(links), 0)

class TestDataBaseClass(unittest.TestCase):

    @patch('wiki_crawl.mysql.connector.connect') # Patch mysql.connector.connect where it's used in wiki_crawl
    @patch('wiki_crawl.DATABASE_CONFIG', {'host': 'test_host', 'user': 'test_user', 'password': 'test_password', 'database': 'test_db'}) # Patch the config variable directly
    def test_db_connection_success(self, mock_connect):
        db = DataBase()
        mock_connect.assert_called_once_with(host='test_host', user='test_user', password='test_password', database='test_db')
        self.assertIsNotNone(db.mydb)
        self.assertIsNotNone(db.cursor)

    @patch('wiki_crawl.mysql.connector.connect') # Patch mysql.connector.connect where it's used in wiki_crawl
    @patch('wiki_crawl.DATABASE_CONFIG', {'host': 'test_host', 'user': 'test_user', 'password': 'test_password', 'database': 'test_db'})
    def test_db_connection_failure(self, mock_connect):
        mock_connect.side_effect = mysql.connector.Error('DB connection failed')
        with self.assertRaises(mysql.connector.Error):
            DataBase()

    @patch('wiki_crawl.mysql.connector.connect') # Patch mysql.connector.connect where it's used in wiki_crawl
    @patch('wiki_crawl.DATABASE_CONFIG', {'host': 'test_host', 'user': 'test_user', 'password': 'test_password', 'database': 'test_db'})
    def test_store_data_success(self, mock_connect):
        mock_mydb = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_mydb
        mock_mydb.cursor.return_value = mock_cursor

        db = DataBase()
        db.store_data('TestGame', 123, '20230101-20230103')

        mock_cursor.execute.assert_called_once()
        mock_mydb.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_mydb.close.assert_called_once()

    @patch('wiki_crawl.mysql.connector.connect') # Patch mysql.connector.connect where it's used in wiki_crawl
    @patch('wiki_crawl.DATABASE_CONFIG', {'host': 'test_host', 'user': 'test_user', 'password': 'test_password', 'database': 'test_db'})
    def test_store_data_failure(self, mock_connect):
        mock_mydb = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_mydb
        mock_mydb.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = mysql.connector.Error("Insert failed")

        db = DataBase()
        with self.assertRaises(mysql.connector.Error):
            db.store_data('TestGame', 123, '20230101-20230103')

        mock_mydb.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_mydb.close.assert_called_once()

    @patch('wiki_crawl.mysql.connector.connect') # Patch mysql.connector.connect where it's used in wiki_crawl
    @patch('wiki_crawl.DATABASE_CONFIG', {'host': 'test_host', 'user': 'test_user', 'password': 'test_password', 'database': 'test_db'})
    def test_close(self, mock_connect):
        mock_mydb = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_mydb
        mock_mydb.cursor.return_value = mock_cursor

        db = DataBase()
        db.close()

        mock_cursor.close.assert_called_once()
        mock_mydb.close.assert_called_once()

class TestDownloadAndProcessFile(unittest.TestCase):

    @patch('wiki_crawl.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    @patch('wiki_crawl.unzip_file', return_value='unzipped_file.txt')
    @patch('wiki_crawl.search_file', return_value=500)
    @patch('wiki_crawl.delete_searched_files')
    def test_download_and_process_file_success(self, mock_delete, mock_search, mock_unzip, mock_open, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [b'chunk1', b'chunk2']
        mock_get.return_value = mock_response

        link = 'pageviews/2023/20230101-hourly.gz'
        search_url = 'http://test.wikipedia.org/downloads/'
        search_term = 'TestPage'
        total_searches = 0

        updated_total = download_and_process_file(link, search_url, search_term, total_searches)

        mock_get.assert_called_once_with(search_url + link, stream=True, timeout=30)
        mock_open.assert_called_once_with('20230101-hourly.gz', 'wb')
        mock_open().write.assert_any_call(b'chunk1')
        mock_open().write.assert_any_call(b'chunk2')
        mock_unzip.assert_called_once_with('20230101-hourly.gz')
        mock_search.assert_called_once_with('unzipped_file.txt', search_term, 0)
        mock_delete.assert_called_once_with(['20230101-hourly.gz', 'unzipped_file.txt'])
        self.assertEqual(updated_total, 500)

    @patch('wiki_crawl.requests.get')
    @patch('wiki_crawl.delete_searched_files')
    def test_download_and_process_file_download_failure(self, mock_delete, mock_get):
        mock_get.side_effect = requests.exceptions.RequestException("Download error")

        link = 'pageviews/2023/20230101-hourly.gz'
        search_url = 'http://test.wikipedia.org/downloads/'
        search_term = 'TestPage'
        total_searches = 0

        updated_total = download_and_process_file(link, search_url, search_term, total_searches)

        mock_delete.assert_called_once_with(['20230101-hourly.gz'])
        self.assertEqual(updated_total, 0)

if __name__ == '__main__':
    unittest.main()