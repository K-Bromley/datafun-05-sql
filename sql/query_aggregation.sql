-- Analysis of the tables

--Total Number of Authors
SELECT COUNT(author_id) AS Number_of_Authors FROM books;

--Oldest Published Book
SELECT MIN(year_published) AS Oldest_Published_Book FROM books;

--Most Recently Published Book
SELECT MAX(year_published) AS Most_Recently_Published_Book FROM books;

--Average Published Year
SELECT AVG(year_published) AS Average_Year_Published FROM books;
