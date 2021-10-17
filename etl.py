# Import external files functions and cassandra_connection
from upload_csv_functions import write_file, read_file, get_files
from cassandra_connection import cassandra_connection, shutdown_connection
from sql_queries import drop_queries, create_queries, insert_queries, select_queries, table_names
from drop_create_insert_select import drop_table, create_table, insert_table, select_values

def main():

    # Call to the functions that: get files path, read the data and write them into a CSV file.
    write_file(full_data_rows_list=read_file(file_path_list=get_files(path='/event_data')))
    
    # Establish connection and session to Cassandra. Also create a keyspace.
    cassandra_connection()
    
    # Call to the fuction that drop tables if they already exist.
    drop_table(drop_queries)
    
    # Call to the function that create tables if not already exist.
    create_table(create_queries)
    
    # Call to the function that insert values into tables.
    insert_table(insert_queries, table_names)
    
    # Call to the function that runs select statements.
    select_values(select_queries, table_names)
    
    # Call to the function that close the connection.
    shutdown_connection()

if __name__ == "__main__":
    main()