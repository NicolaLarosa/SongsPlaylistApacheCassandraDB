# DROP TABLE QUERIES
drop_event_table_1 = "DROP TABLE IF EXISTS song_playlist_session" 
drop_event_table_2 = "DROP TABLE IF EXISTS artist_songs_user_session"
drop_event_table_3 = "DROP TABLE IF EXISTS users_song"

# CREATE TABLE QUERIES
create_event_table_1 = "CREATE TABLE IF NOT EXISTS song_playlist_session ( sessionId int, itemInSession int, artist text, length double, song text, PRIMARY KEY(sessionId, itemInSession))"
create_event_table_2 = "CREATE TABLE IF NOT EXISTS artist_songs_user_session (userid int, sessionid int, itemInSession int, artist text, song text, firstName text, lastName text,\
PRIMARY KEY((userid, sessionid), itemInSession)) WITH CLUSTERING ORDER BY (itemInSession ASC)"
create_event_table_3 = "CREATE TABLE IF NOT EXISTS users_song (song text, user_id int, firstName text, lastName text, PRIMARY KEY(song, user_id))"

# INSERT INTO TABLE QUERIES
insert_event_table_1 = "INSERT INTO song_playlist_session (sessionId, itemInSession, artist, length, song) VALUES (%s,%s,%s,%s,%s)" 
insert_event_table_2 = "INSERT INTO artist_songs_user_session (userid, sessionid, itemInSession, artist, song, firstName, lastName) VALUES (%s,%s,%s,%s,%s,%s,%s)"
insert_event_table_3 = "INSERT INTO users_song (song, user_id, firstName, lastName) VALUES (%s,%s,%s,%s)"

# SELECT STATEMENT QUERIES
select_event_table_1 = "SELECT artist, song, length FROM song_playlist_session WHERE sessionId = 338 and itemInSession = 4" 
select_event_table_2 = "SELECT artist, song, firstName, lastName FROM artist_songs_user_session WHERE userid = 10 AND sessionid = 182 ORDER BY itemInSession ASC"
select_event_table_3 = "SELECT firstName, lastName FROM users_song WHERE song = 'All Hands Against His Own'"

table_names = ['song_playlist_session', 'artist_songs_user_session', 'users_song'] # List of table name
drop_queries = [drop_event_table_1, drop_event_table_2, drop_event_table_3] # List of drop queries
create_queries = [create_event_table_1, create_event_table_2, create_event_table_3] # List of create queries
insert_queries = [insert_event_table_1, insert_event_table_2, insert_event_table_3] # list of insert queries
select_queries = [select_event_table_1, select_event_table_2, select_event_table_3] # List of select statements