# wikipedia_data_search
July 2025 Update:

Testing some LLM workflows, I converted this into a "Vibe Code" project. The program still crawls Wikipedia's data files with BeautifulSoup to find trends on Wikipedia's main website, but vibe coding added some features, error handling, and tests
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
- Wikipedia gets approximately 3-6 million searches evey hour and it takes about 10 minutes to search data for one full day on a really good consumer PC (30 mins on laptop)<br>

