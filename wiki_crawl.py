import logging
import gzip
import shutil
import os
import requests
import pandas as pd
import mysql.connector
from bs4 import BeautifulSoup
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    from config import DATABASE_CONFIG, WIKIPEDIA_BASE_URL
except ImportError:
    logging.error('config.py not found or DATABASE_CONFIG/WIKIPEDIA_BASE_URL not defined. Please create config.py with these variables.')
    exit(1)

class Wiki:
    def __init__(self, search_url):
        self.search_url = search_url
        self.soup = None

    def fetch_page(self):
        try:
            response = requests.get(self.search_url, timeout=10)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
            self.soup = BeautifulSoup(response.text, 'html.parser')
            logging.info(f'Successfully fetched page: {self.search_url}')
        except requests.exceptions.RequestException as e:
            logging.error(f'Error fetching URL {self.search_url}: {e}')
            raise

    def find_all_links(self) -> list:
        if not self.soup:
            logging.warning('No soup object available. Call fetch_page() first.')
            return []
        return self.soup.find_all('a')

class DataBase:
    def __init__(self):
        self.mydb = None
        self.cursor = None
        try:
            self.mydb = mysql.connector.connect(**DATABASE_CONFIG)
            self.cursor = self.mydb.cursor()
            logging.info('Successfully connected to the database.')
        except mysql.connector.Error as err:
            logging.error(f'Error connecting to database: {err}')
            raise

    def store_data(self, searching_term, search_total, dates_searched) -> None:
        sql = '''INSERT INTO games (game_title,
                                    total_searches,
                                    search_dates)
                 VALUES (%s, %s, %s)'''
        val = (searching_term, str(search_total), dates_searched)
        try:
            self.cursor.execute(sql, val)
            self.mydb.commit()
            logging.info(f'Successfully stored data for "{searching_term}" with total searches {search_total}.')
        except mysql.connector.Error as err:
            logging.error(f'Error storing data: {err}')
            self.mydb.rollback()
            raise
        finally:
            self.close()

    def close(self):
        if self.cursor:
            self.cursor.close()
            logging.info('Database cursor closed.')
        if self.mydb:
            self.mydb.close()
            logging.info('Database connection closed.')

def format_url(url_base: str, format_year: str, format_month: str) -> str:
    return f'{url_base}{format_year}/{format_year}-{format_month}/'

def validate_date_input(year_str: str, month_str: str, start_day_str: str, end_day_str: str) -> tuple:
    try:
        year = int(year_str)
        month = int(month_str)
        start_day = int(start_day_str)
        end_day = int(end_day_str)

        if not (2015 <= year <= datetime.now().year):
            raise ValueError('Year must be between 2015 and current year.')
        if not (1 <= month <= 12):
            raise ValueError('Month must be between 1 and 12.')
        if not (1 <= start_day <= 31 and 1 <= end_day <= 31):
            raise ValueError('Day must be between 1 and 31.')
        if start_day > end_day:
            raise ValueError('Start day cannot be after end day.')

        # Basic check for days in month, more robust check can be added
        if month in [4, 6, 9, 11] and (start_day > 30 or end_day > 30):
            raise ValueError('Invalid day for this month (max 30).')
        if month == 2:
            if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0): # Leap year
                if start_day > 29 or end_day > 29:
                    raise ValueError('Invalid day for February (max 29 in leap year).')
            else:
                if start_day > 28 or end_day > 28:
                    raise ValueError('Invalid day for February (max 28).')

        return year, month, start_day, end_day
    except ValueError as e:
        logging.error(f'Invalid date input: {e}')
        raise

def get_search_span(year: int, month: int, start_day: int, end_day: int) -> list:
    list_of_dates = []
    for day in range(start_day, end_day + 1):
        list_of_dates.append(f'{year}{month:02d}{day:02d}')
    return list_of_dates

def display_search_dates(search_range: list) -> str:
    if not search_range:
        return 'No dates to display.'
    if len(search_range) > 1:
        display_dates = f'{search_range[0]} - {search_range[-1]}'
    else:
        display_dates = search_range[0]
    logging.info(f'Performing search for following date(s): {display_dates}')
    return display_dates

def write_links_to_file(wiki_links: list, search_dates: list, filename: str = 'wiki_links.txt') -> None:
    try:
        with open(filename, 'w') as wiki_file:
            for link in wiki_links:
                href = link.get('href')
                if href and href.startswith('pageviews'):
                    for day in search_dates:
                        if day in href:
                            wiki_file.write(href + '\n')
        logging.info(f'Successfully wrote links to {filename}')
    except IOError as e:
        logging.error(f'Error writing links to file {filename}: {e}')
        raise

def unzip_file(file_name: str) -> str:
    output_filename = f'{file_name}.txt'
    try:
        with gzip.open(file_name, 'rb') as file_in:
            with open(output_filename, 'wb') as file_out:
                shutil.copyfileobj(file_in, file_out)
        logging.info(f'Successfully unzipped {file_name} to {output_filename}')
        return output_filename
    except Exception as e:
        logging.error(f'Error unzipping file {file_name}: {e}')
        raise

def search_file(text_file: str, searching_term: str, current_total_searches: int) -> int:
    try:
        file_df = pd.read_csv(text_file, sep=' ', header=None, on_bad_lines='skip')
        file_df.columns = ['Domain Code', 'Page Title', 'Count Views', 'Response Size']

        # Ensure 'Count Views' is numeric, coercing errors to NaN
        file_df['Count Views'] = pd.to_numeric(file_df['Count Views'], errors='coerce')
        file_df.dropna(subset=['Count Views'], inplace=True) # Drop rows where conversion failed

        hourly_views = file_df[file_df['Page Title'] == searching_term]['Count Views'].sum()
        new_total_searches = current_total_searches + hourly_views

        logging.info(f'"{searching_term}" has been searched on Wikipedia {int(hourly_views)} times this hour.')
        logging.info(f'"{searching_term}" has a total of {int(new_total_searches)} searches so far.')
        return int(new_total_searches)
    except FileNotFoundError:
        logging.error(f'File not found: {text_file}')
        return current_total_searches
    except pd.errors.EmptyDataError:
        logging.warning(f'File {text_file} is empty or malformed, skipping search.')
        return current_total_searches
    except Exception as e:
        logging.error(f'Error searching file {text_file}: {e}')
        return current_total_searches

def delete_searched_files(file_paths: list) -> None:
    for file_path in file_paths:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logging.info(f'Successfully deleted file: {file_path}')
            else:
                logging.warning(f'File not found for deletion: {file_path}')
        except OSError as e:
            logging.error(f'Error deleting file {file_path}: {e}')

def download_and_process_file(link: str, search_url: str, search_term: str, total_searches: int) -> int:
    filename_raw = link.split('/')[-1]
    filename = filename_raw.strip() # Remove any trailing newlines or spaces
    full_download_url = search_url + link.strip()

    logging.info(f'Downloading {filename} from {full_download_url}')
    try:
        response = requests.get(full_download_url, stream=True, timeout=30)
        response.raise_for_status()
        with open(filename, 'wb') as output_file:
            for chunk in response.iter_content(chunk_size=8192):
                output_file.write(chunk)
        logging.info(f'Successfully downloaded {filename}')

        unzipped_file_path = unzip_file(filename)
        updated_total_searches = search_file(unzipped_file_path, search_term, total_searches)
        delete_searched_files([filename, unzipped_file_path])
        return updated_total_searches
    except requests.exceptions.RequestException as e:
        logging.error(f'Error downloading {full_download_url}: {e}')
        delete_searched_files([filename]) # Clean up partially downloaded file
        return total_searches
    except Exception as e:
        logging.error(f'Error processing file {filename}: {e}')
        delete_searched_files([filename, unzipped_file_path]) # Clean up if unzipping or searching failed
        return total_searches

def main():
    logging.info('Starting Wikipedia data search script.')
    print('Wikipedia data is offered from year 2015'.upper())

    try:
        year_input = input('Enter year for your search[YYYY]: ')
        month_input = input('Enter month for your search[MM]: ')
        start_day_input = input('Enter a day to start your search[DD]: ')
        end_day_input = input('Enter a day to end your search[DD]: ')

        year, month, start_day, end_day = validate_date_input(year_input, month_input, start_day_input, end_day_input)
    except ValueError:
        logging.error('Exiting due to invalid date input.')
        return

    search_span = get_search_span(year, month, start_day, end_day)
    search_dates_display = display_search_dates(search_span)
    search_term = input('Enter word to search: ')

    search_url = format_url(WIKIPEDIA_BASE_URL, str(year), f'{month:02d}')

    try:
        wiki = Wiki(search_url)
        wiki.fetch_page()
        all_links = wiki.find_all_links()
        write_links_to_file(all_links, search_span, 'wiki_links.txt')
    except Exception as e:
        logging.error(f'Failed to initialize Wiki or fetch links: {e}')
        return

    total_searches = 0
    try:
        with open('wiki_links.txt', 'r') as links_file:
            for link in links_file:
                if link.strip(): # Ensure link is not empty
                    total_searches = download_and_process_file(link, search_url, search_term, total_searches)
    except FileNotFoundError:
        logging.error('wiki_links.txt not found. No files to process.')
        return
    except Exception as e:
        logging.error(f'Error processing links from file: {e}')
        return
    finally:
        delete_searched_files(['wiki_links.txt']) # Clean up the links file

    try:
        database = DataBase()
        database.store_data(search_term, total_searches, search_dates_display)
    except Exception as e:
        logging.error(f'Failed to store data in database: {e}')

    logging.info('Script finished.')

if __name__ == '__main__':
    main()