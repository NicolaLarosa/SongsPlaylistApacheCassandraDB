# Import Python packages 
import pandas as pd
import re
import os
import glob
import numpy as np
import json
import csv

def write_file(full_data_rows_list):
    '''
    Write data into a file.
    
    Given a list of rows full of data previously red as input parameter,\
    this function write these info into a CSV file named event_datafile_new.
    
    Parameters:
    full_data_rows_list (list): List that contains all the info to be written into a CSV file.
    
    Returns:
    No returned parameters.
    '''
    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    # Open the CSV file in writing mode
    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        # for loop to write all the rows of full_data_rows_list into event_datafile_new.csv file.
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
        print('\nWRITING: {} lines written into event_datafile_new, File size: {}\n'.format(sum(1 for line in f), os.path.getsize("event_datafile_new.csv")))


def read_file(file_path_list):
    '''
    Read CSV files.
    
    Given a list of file paths, this function reads all the data from these files\
    and stores them into a list that will be returned as output parameter.
    
    Parameters:
    file_path_list (list): contains all the CSV file names and their path.
    
    Returns:
    full_data_rows_list (list): List that contains all the info stored in different CSV files. 
    '''
    full_data_rows_list = []
    # for to loop on every file in the files path
    for f in file_path_list:

        # Open the CSV file in reading mode
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            # creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            next(csvreader)

             # extracting each data row one by one and append it        
            for line in csvreader:
                #print(line)
                full_data_rows_list.append(line) 

    print('\nREADING: {} lines have been read from full_data_rows_list\n'.format(len(full_data_rows_list)))
    
    return full_data_rows_list

def get_files(path):
    '''
    Gets files stored in filepath.
    
    This function is meant to get all files matching .json extension from the directory which the path is specified in filepath.
    Then it iterates over all files printing the file number that is being processed out of the total number of files found in directory.
    
    Parameters:
    path (text): it is the path to the folder which contains the CSV files.
    
    Returns:
    file_path_list (list): List that contains every CSV file name and its path.
    '''
    # checking your current working directory
    print(os.getcwd())

    # Get your current folder and subfolder event data
    filepath = os.getcwd() + path

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):

    # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
        #print(file_path_list)
    print('Red {} files in {}'.format(len(file_path_list), filepath))
    return file_path_list
