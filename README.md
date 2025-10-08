# Wikipedia Data Search

This project is a Python-based tool for downloading and analyzing Wikipedia's publicly available page view data to identify trends. It allows users to specify a date range and a search term, and the program will crawl `dumps.wikimedia.org`, download the relevant data, and count the occurrences of the search term. The results are then stored in a MySQL database.

## Features

*   **Date-based Searching:** Specify a year, month, and day range to search for data within a specific period.
*   **Term Frequency Analysis:**  Counts the number of times a specific term has been searched for on Wikipedia.
*   **Data Downloading and Processing:** Automatically downloads, unzips, and processes the gzipped data files from Wikipedia.
*   **Database Integration:** Stores the search term, total count, and search dates into a MySQL database for later analysis.
*   **Efficient Memory Usage:** Deletes the unzipped files after processing to save disk space.
*   **Error Handling:** Includes error handling for network requests, file operations, and database interactions.
*   **Logging:** Logs important events and errors to the console.

## How It Works

1.  **User Input:** The program prompts the user to enter a year, month, start day, end day, and a search term.
2.  **URL Generation:** Based on the user's input, the program constructs the URL to crawl for the specified date range.
3.  **Web Scraping:** The tool uses `BeautifulSoup` to scrape the Wikipedia dumps page and find the relevant data file links.
4.  **File Filtering:** The program filters the links to find the ones that match the specified date range and saves them to a local file.
5.  **Data Processing:** The script iterates through the list of file links and for each file:
    *   Downloads the gzipped file.
    *   Unzips the file.
    *   Uses `pandas` to read the data and count the occurrences of the search term.
    *   Deletes the downloaded and unzipped files.
6.  **Database Storage:** After processing all the files, the program connects to a MySQL database and stores the search term, the total count, and the date range of the search.

## Requirements

*   Python 3.x
*   `requests`
*   `beautifulsoup4`
*   `pandas`
*   `mysql-connector-python`

## Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/your-username/wikipedia_data_search.git
    cd wikipedia_data_search
    ```
2.  Install the required packages:
    ```bash
    pip install -r requirements.txt
    ```
3.  Set up your database configuration in a `config.py` file. Create a `config.py` file in the root directory and add the following variables:
    ```python
    DATABASE_CONFIG = {
        'user': 'your_username',
        'password': 'your_password',
        'host': 'your_host',
        'database': 'your_database'
    }

    WIKIPEDIA_BASE_URL = 'https://dumps.wikimedia.org/other/pageviews/'
    ```

## Usage

1.  Run the `wiki_crawl.py` script:
    ```bash
    python wiki_crawl.py
    ```
2.  Follow the on-screen prompts to enter the year, month, day range, and the search term.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue if you have any suggestions or find any bugs.