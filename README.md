# Database Modeling with Apache Cassandra

## Project overview:

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

## Purpose of the project:

The purpose of this project is creating an Apache Cassandra database to run queries provided by Sparkify analytics team in order to answer few questions.
This project will be divided as follow:
1. Retrieve the data from several CSV files stored in an external folder.
2. Create a connection to Cassandra DB.
3. Run the following DDL, DML CQL statement:
    1. DROP TABLE IF EXIST statement.
    2. CREATE TABLE statement.
    3. INSERT INTO TABLE statement.
    4. SELECT statement.

## Project files:

Target of each ile:
1. **upload_csv_functions.py**, its main targets are to retrieve the data (stored in several CSV files) in an external folder, and then write all this info into a new CSV file named event_datafile_new.csv containing all the info.
2. **cassandra_connection.py**, its main target is to establish a connection to a local instance of Apache Cassandra DB.
3. **sql_queries.py**, this file contains all the DDL, DML statement useful to _DROP_ tables, _CREATE_ tables, _INSERT_ values into tables and _SELECT_ values to be retrieved.
4. **drop_create_insert_select.py**, its main targets is to provide tables filled by all the values needed to be queried by the queries previously provided.
5. **etl.py**, its main target is calling the functions created in the other .py files in order to provide the result asked by the Sparkify analytics team.

## ETL Pipeline:

1. **upload_csv_functions.py** contains the following functions:
    1. get_files: given a folder path as an input parameter, it retrieves the files path (path of the file plus CSV file name) and returns a list with one file path for each CSV file.
    2. read_file: given a list of files path as input parameter, it reads all the data contained in a specific file (line by line) and inserts it into a new list which aims to contain all the data coming from different CSV files.
    3. write_file: given a list containing all the data as input parameter, it writes the content of this list into a CSV file named event_datafile_new.
2. **cassandra_connection.py** contains the following functions:
    1. cassandra_connection: this function is meant to connect to Apache Cassandra local machine (127.0.0.1), initialize its session and finally create a KEYSPACE.
    2. shutdown_connection: this function shutdown the connection to Apache Cassandra DB.
3. **sql_queries.py** this file provides:
    1. all the DDL and DML statement for each table.
    2. five lists (table_names, drop_queries, create_queries, insert_queries, select_queries) containing the table names and DDL, DML statement that will be imported by the other _.py_ files.
4. **drop_create_insert_select.py** contains the following functions:
    1. drop_table: given a _DROP_ statement queries list as an input parameter, it establishes the connection and the session to Apache Cassandra and drop all the tables.
    2. create_table: given a _CREATE_ statement queries list as an input parameter, it establishes the connection and the session to Apache Cassandra and create all the tables needed.
    3. insert_table: given a _INSERT_ statement list and a table name list as input parameters, it runs the specific insert statement to its table.  
    4. select_table: given a _INSERT_ statement list and a table name list as input parameters, it runs the select statement according to its table.

The relation with these .py files is described as follow:
1. **cassandra_connection.py** is imported in drop_create_insert_select.py, etl.py.
2. **drop_create_insert_select.py** is imported in etl.py.
3. **sql_queries** is imported in etl.py.
4. **upload_csv_functions** is imported in etl.py.

## Queries:

These are the queries that have been run and their results:

1. _**SELECT artist, song, length FROM event_1 WHERE sessionId = '338' and itemInSession = '4'**_ 

Faithless Music Matters (Mark Knight Dub) 495.3073

2. _**SELECT artist, song, firstName, lastName FROM event_2 WHERE userid = '10' AND sessionid = '182' ORDER BY itemInSession ASC**_

Down To The Bone Keep On Keepin' On Sylvie Cruz
Three Drives Greece 2000 Sylvie Cruz
Sebastien Tellier Kilometer Sylvie Cruz
Lonnie Gordon Catch You Baby (Steve Pitron & Max Sanna Radio Edit) Sylvie Cruz

3. _**SELECT firstName, lastName FROM event_3 WHERE song = 'All Hands Against His Own'**_

Jacqueline Lynch