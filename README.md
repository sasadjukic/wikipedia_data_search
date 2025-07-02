# wikipedia_data_search
July 2025 Update:

Testing some LLM workflows I converted this into a "Vibe Code" project. The proggram still crawls Wikipedia's data files with BeautifulSoup to find trends on Wikipedia's main website, but vibe coding added some features and tests
<br>

<b>Basic steps the program executes are:</b>
1. The program asks user to input search dates and a searh term 
2. Based on the input the program crawls dumps.wikimedia.org, and saves all relevant link names to a separate text file  
3. The program downloads zipped wikipedia's files that match link names from the text file
4. The program unzips the files one by one
5. Using Pandas, the program searches for the term specified by the user and counts how many times the term has been searched
7. After the search and after the count has been stored, the program deletes the unzipped file to save disk space
8. Repeat steps 3 to 6 until all files have been exhausted
9. The search term, the count, and the search date(s) are stored into MySQL database

<b>NOTES</b>
<br>
- I used print statments for the command prompt to keep updating the user what exact file is beaing searched at the moment and how many searches the keyword has accrued so far
- Wikipedia gets approximately 3-6 million searches evey hour and it takes about 10 minutes to search data for one full day on a good consumer PC<br>
- Few of the many of hourly files I searched caused an error while reading. Due to me not being able to find what exactly causes the read error I used try/except to handle those files. Reading numbers from those files are not included in the total tally.
