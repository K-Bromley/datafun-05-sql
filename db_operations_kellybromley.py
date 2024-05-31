import sqlite3
import pandas as pd
import pathlib
import logging

# Configure logging to write to a file, appending new logs to the existing file

logging.basicConfig(filename='project_5_sql.log', level=logging.DEBUG, encoding='utf-8')

db_file = pathlib.Path('project.db')

# Create Tables
def create_tables():
    """Function to read and execute SQL statements to create tables"""
    try:
        with sqlite3.connect(db_file) as conn:
            sql_file = pathlib.Path("sql", "create_tables.sql")
            with open(sql_file, "r") as file:
                sql_script = file.read()
            conn.executescript(sql_script)
            print("Tables created successfully.")
    except sqlite3.Error as e:
        print("Error creating tables:", e)

# Insert CSV data
def insert_data_from_csv():
    """Function to use pandas to read data from CSV files (in 'data' folder)
    and insert the records into their respective tables."""
    try:
        author_data_path = pathlib.Path("data", "authors.csv")
        book_data_path = pathlib.Path("data", "books.csv")
        authors_df = pd.read_csv(author_data_path)
        books_df = pd.read_csv(book_data_path)
        with sqlite3.connect(db_file) as conn:
            # use the pandas DataFrame to_sql() method to insert data
            # pass in the table name and the connection
            authors_df.to_sql("authors", conn, if_exists="replace", index=False)
            books_df.to_sql("books", conn, if_exists="replace", index=False)
            print("Data inserted successfully.")
    except (sqlite3.Error, pd.errors.EmptyDataError, FileNotFoundError) as e:
        print("Error inserting data:", e)

# Insert Records
def execute_insert_records(db_file, insert_records_sql):
    """Function for inserting new records into the data tables"""
    with sqlite3.connect(db_file) as conn:
        insert_records_sql = pathlib.Path("sql", "insert_records.sql")
    with open(insert_records_sql, 'r') as file:
            sql_script = file.read()
    conn.executescript(sql_script)
    print(f"Executed SQL from {insert_records_sql}")

# Delete Records
def execute_delete_records(db_file, delete_records_sql):
    """Function for deleting records from the data tables"""
    with sqlite3.connect(db_file) as conn:
        delete_records_sql = pathlib.Path("sql", "delete_records.sql")
    with open(delete_records_sql, 'r') as file:
            sql_script = file.read()
    conn.executescript(sql_script)
    print(f"Executed SQL from {delete_records_sql}")

# Update Records
def execute_update_records(db_file, update_records_sql):
    """Function for updating records from the data tables"""
    with sqlite3.connect(db_file) as conn:
        update_records_sql = pathlib.Path("sql", "update_records.sql")
    with open(update_records_sql, 'r') as file:
            sql_script = file.read()
    conn.executescript(sql_script)
    print(f"Executed SQL from {update_records_sql}")

# Query Aggregate
def execute_query_aggregation(db_file, query_aggregation_sql):
    """Function for performing aggregation of data from tables"""
    with sqlite3.connect(db_file) as conn:
        query_aggregation_sql = pathlib.Path("sql", "query_aggregation.sql")
    with open(query_aggregation_sql, 'r') as file:
            sql_script = file.read()
    conn.executescript(sql_script)
    print(f"Executed SQL from {query_aggregation_sql}")

# Query Filter
def execute_query_filter(db_file, query_filter_sql):
    """Function for filtering data within the tables"""
    with sqlite3.connect(db_file) as conn:
        query_filter_sql = pathlib.Path("sql", "query_filter.sql")
        with open(query_filter_sql, 'r') as file:
            sql_script = file.read()
        conn.executescript(sql_script)
        print(f"Executed SQL from {query_filter_sql}")

# Query Sorting
def execute_query_sorting(db_file, query_sorting_sql):
    """Function for sorting data within the tables"""
    with sqlite3.connect(db_file) as conn:
        query_sorting_sql = pathlib.Path("sql", "query_sorting.sql")
        with open(query_sorting_sql, 'r') as file:
            sql_script = file.read()
        conn.executescript(sql_script)
        print(f"Executed SQL from {query_sorting_sql}")

# Query Group
def execute_query_group_by(db_file, query_group_by_sql):
    """Function for grouping data within the tables"""
    with sqlite3.connect(db_file) as conn:
        query_group_by_sql = pathlib.Path("sql", "query_group_by.sql")
        with open(query_group_by_sql, 'r') as file:
            sql_script = file.read()
        conn.executescript(sql_script)
        print(f"Executed SQL from {query_group_by_sql}")

# Query Join 
def execute_query_join(db_file, query_join_sql):
    """Function for joining data from 2 separate tables"""
    with sqlite3.connect(db_file) as conn:
        query_join_sql = pathlib.Path("sql", "query_join.sql")
        with open(query_join_sql, 'r') as file:
            sql_script = file.read()
        conn.executescript(sql_script)
        print(f"Executed SQL from {query_join_sql}")


def main():
    
    logging.info("Program started") # add this at the beginning of the main method
    
    db_file = pathlib.Path ('project.db')
    
    # Create database schema and populate with data
    create_tables()
    insert_data_from_csv()
    execute_insert_records(db_file, 'insert_records_sql')
    execute_update_records(db_file,'update_records_sql')
    execute_delete_records(db_file, 'delete_records_sql')
    execute_query_aggregation(db_file, 'query_aggregation_sql')
    execute_query_filter(db_file, 'query_filter_sql')
    execute_query_sorting(db_file, 'query_sorting_sql')
    execute_query_group_by(db_file, 'query_group_by_sql')
    execute_query_join(db_file, 'query_join_sql')

    logging.info("All SQL operations completed successfully")
    
    logging.info("Program ended")  # add this at the end of the main method
    
if __name__ == '__main__':
    main()
    