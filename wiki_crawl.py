
from bs4 import BeautifulSoup
import pandas as pd
import gzip, shutil, os, requests
import mysql.connector

class Wiki:
    
    def __init__(self, search_url):

        self.URL = requests.get(f'{search_url}')
        web_page = self.URL.text
        self.soup = BeautifulSoup(web_page, 'html.parser')

    def find_all_links(self) -> list:

        self.all_links = self.soup.find_all('a')
        return self.all_links

class DataBase:
    
    def __init__(self):

        self.mydb = mysql.connector.connect(
            host = 'host',
            user = 'user',
            password = 'password',
            database = 'GameTrends'
            )

        self.cursor = self.mydb.cursor()
    
    def store_data(self, searching_term, search_total, dates_searched) -> None:

        self.sql = '''INSERT INTO games (game_title,
                                        total_searches,
                                        search_dates)
                      Values (%s, %s, %s)'''

        self.val = (searching_term, str(search_total), dates_searched)

        self.cursor.execute(self.sql, self.val)
        self.mydb.commit()

def format_url(url_base, format_year, format_month) -> str:

    year_month = format_year + '-' + format_month
    formatted_url = url_base + format_year + '/' + year_month + '/'

    return formatted_url

def get_search_span(search_year, search_month) -> list:
    list_of_dates = []
    start_date = input('Enter a day to start your search[DD]: ')
    end_date = input('Enter a day to end your search[DD]: ')

    f_num = start_date[-2:]
    s_num = end_date[-2:]

    if f_num[0] == '0':
        f_new_number = f_num[1]
        f_num = f_new_number
    if s_num[0] == '0':
        s_new_number = s_num[1]
        s_num = s_new_number

    for number in range(int(f_num), int(s_num)+1):
        if number < 10:
            list_of_dates.append(f'{search_year}{search_month}0{number}')
        else:
            list_of_dates.append(f'{search_year}{search_month}{number}')

    return list_of_dates

def display_search_dates(search_range) -> str:
    if len(search_range) > 1:
        display_dates = str(search_range[0]) + '-' + str(search_range[-1])
    else:
        display_dates = str(search_range[0])

    print(f'Performing search for following date(s): {display_dates}')

    return display_dates

def write_links_to_file(wiki_links, search_dates) -> None:

    with open('wiki_links.txt', 'w') as wiki_file:
        for link in wiki_links:
            if link['href'].startswith('pageviews'):
                for day in search_dates:
                    if day in link['href']:
                        wiki_file.write(link['href'] + '\n')

def unzip_file(file_name) -> str:
    file_in = gzip.open(f'{file_name}', 'rb')
    file_out = open(f'{file_name}.txt', 'wb')
    shutil.copyfileobj(file_in, file_out)
    file_in.close()
    file_out.close()
    return file_out

def search_file(text_file, searching_term, search_total) -> int:

    try:
        file = pd.read_csv(text_file, sep = ' ', header= None)
        file.columns = ['Domain Code', 'Page Title', 'Count Views', 'Response Size']

        index_counter = 0
        count_hourly_views = 0

        for title in file['Page Title']:
            if title == searching_term:
                count_hourly_views += file.loc[file.index[index_counter], 'Count Views']
            index_counter += 1

        search_total += count_hourly_views
        print(f'\n{searching_term} has been searched on Wikipedia {count_hourly_views} times this hour')
        print(f'{searching_term} has total of {search_total} searches so far')

    except:
        print('\nError'.upper() + f' reading file {text_file}')

    return (search_total)

def delete_searched_files(file, named_file) -> None:
    if os.path.exists(f'{file}') and os.path.exists(f'{named_file}'):
        os.remove(f'{file}')
        os.remove(f'{named_file}')
    else:
        print('The file does not exist')

def main():

    url_base = 'https://dumps.wikimedia.org/other/pageviews/'
    prompt = print('Wikipedia data is offered from year 2015'.upper())
    year = input('Enter year for your search[YYYY]: ')
    month = input('Enter month for your search[MM]: ')
    search_span = get_search_span(year, month)
    search_dates = display_search_dates(search_span)
    search_term = input('Enter word to search: ')
    search_url = format_url(url_base, year, month)
    wiki = Wiki(search_url)
    all_links = wiki.find_all_links()
    write_links_to_file(all_links, search_span)

    with open('wiki_links.txt', 'r') as links:
        total_searches = 0
        for link in links:
            if link:
                filename_raw = link.split('/')[-1]
                filename = filename_raw[:-1]
                link = search_url + link
                print(filename + ' file started to download')
                response = requests.get(link[:-1])

                with open(filename, 'wb') as output_file:
                    output_file.write(response.content)
                print(filename + ' file is' + ' downloaded'.upper())

                file_out = unzip_file(filename)
                total_searches = search_file(file_out.name, search_term, total_searches)
                delete_searched_files(file_out.name, filename)

    database = DataBase()
    database.store_data(search_term, total_searches, search_dates)

if __name__ == '__main__':
    main()