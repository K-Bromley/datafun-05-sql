-- Joining tables

SELECT first_name, last_name
FROM authors
INNER JOIN books 
    ON authors.author_id = books.author_id