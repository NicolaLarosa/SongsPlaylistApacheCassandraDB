import pandas as pd
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra_connection import cassandra_connection

def drop_table(queries):
    '''
    Drops tables.
    
    This function aims to drop tables if they already exists.\
    It takes a list of drop statement queries as input parameter.
    
    Parameters:
    queries (list): it contains the queries that need to be run in order to drop the already existing tables. 
    
    Returns:
    No parameters returned.
    '''
    
    try:
        print("DROP TABLES:\n\n")        
        session = cassandra_connection() # Establish session
        # for loop to execute all the drop statements in the queries list.
        for query in queries:
            session[0].execute(query)
            print("\nExecuted: {}\n".format(query))
    
    except Exception as e:
        print(e)

def create_table(queries):
    '''
    Create tables.
    
    This function creates tables if they already do not exist,\
    taking as input parameter a list of create statement queries.
    
    Parameters:
    queries (list): it contains the queries that need to be run\
    in order to create tables if not already exist.
        
    Returns:
    No returned parameters.
    '''
    
    try:
        print("CREATE TABLES:\n\n")
        session = cassandra_connection() # Establish session
        # for loop to execute all the create statements in the queries list.
        for query in queries:
            session[0].execute(query)
            print("\nExecuted: {}\n".format(query))
    
    except Exception as e:
        print(e)
        
def insert_table(insert_queries, table_names):
    '''
    Insert values into tables.
    
    This function reads values from a file, and then it writes them into a table\
    using an insert statement query passed as input parameter.
    
    Parameters:
    insert_queries (list): it contains the queries that need to be run\
    in order to insert values into a specific table.
    table_names (list): it contains the name of the tables that need to be filled.
    
    Returns:
    No returned parameters.
    
    '''
    print("START INSERT STATEMENTS:\n\n")
    file = os.getcwd() + '/event_datafile_new.csv'
    session = cassandra_connection() # Establish session
    # for to loop on all table names
    for table_name in table_names:
        # if statement to check the table name
        if table_name == 'song_playlist_session':
            # Open the csv file in read mode
            with open(file, encoding = 'utf8') as f:
                csvreader = csv.reader(f)
                next(csvreader) # skip header
                # for to loop on all the lines of the csv file
                for line in csvreader:
                    try:
                        # execute the insert statement
                        session[0].execute(insert_queries[0], (int(line[8]), int(line[3]), line[0], float(line[5]), line[9]))
                    except Exception as e:
                        print(e)
            
            print("\nExecuted: {}\n".format(insert_queries[0]))
        # elif statement to check the table name
        elif table_name == 'artist_songs_user_session':
            # Open the csv file in read mode
            with open(file, encoding = 'utf8') as f:
                csvreader = csv.reader(f)
                next(csvreader) # skip header
                # for to loop on all the lines of the csv file
                for line in csvreader:
                    try:
                        # execute the insert statement
                        session[0].execute(insert_queries[1], (int(line[10]),int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))
                    except Exception as e:
                        print(e)
            
            print("\nExecuted: {}\n".format(insert_queries[1]))
        # elif statement to check the table name
        elif table_name == 'users_song':
            # Open the csv file in read mode
            with open(file, encoding = 'utf8') as f:
                csvreader = csv.reader(f)
                next(csvreader) # skip header
                # for to loop on all the lines of the csv file
                for line in csvreader:
                    try:
                        # execute the insert statement
                        session[0].execute(insert_queries[2], (line[9], int(line[10]), line[1], line[4]))
                    except Exception as e:
                        print(e)
            
            print("\nExecuted: {}\n".format(insert_queries[2]))
        
        else:
            print('Error: No valid parameter {} for table_name\n'.format(table_name))

    print("END INSERT STATEMENTS:\n\n")

def select_values(select_queries, table_names):
    '''
    Run select statement queries to retrieve values.
    
    This function runs select statements to retrieve data from tables passed as list input pararmeter.\
    It takes a list of select statement queries as input parameter, and it runs a specific query\
    based on the table name.
    
    Parameters:
    select_queries (list): List of select statement queries.
    table_names (list): List of table names.
    
    Returns:
    No returned parameters.
    '''
    
    table_names_list = table_names # assign the list input parameter to table_names_list
    session = cassandra_connection() # Establish session 
    print("START SELECT STATEMENTS:\n\n")
    # for to loop on all table names
    for table_name in table_names_list:
        # if statement to check the table name
        if table_name == 'song_playlist_session':
            
            query = select_queries[0]
            print("{}\n\n".format(query))   
            
            try:
                # execute select statement
                rows = session[0].execute(query)
                for row in rows:
                    print (row[0], row[1], row[2])
                print("\n")                
            except Exception as e:
                print(e)
        # elif statement to check the table name
        elif table_name == 'artist_songs_user_session':
            
            query = select_queries[1]
            print("\nExecuted: {}\n".format(query))
            
            try:
                # execute select statement
                rows = session[0].execute(query)
                for row in rows:
                    print (row[0], row[1], row[2], row[3])
                print("\n")                
            except Exception as e:
                print(e)
        # elif statement to check the table name
        elif table_name == 'users_song':
            
            query = select_queries[2]
            print("\nExecuted: {}\n".format(query))
            
            try:
                # execute select statement
                rows = session[0].execute(query)
                for row in rows:
                    print (row[0], row[1])
                print("\n")
            except Exception as e:
                print(e)
        
        else:
            print('Error: No valid parameter {} for table_name'.format(table_name))

    print("END SELECT STATEMENTS:\n\n")