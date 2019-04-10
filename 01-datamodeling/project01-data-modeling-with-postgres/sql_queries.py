# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

# song_id and artist_id haven't constraints NOT NULL because the sample data to test ETL doesn't have all the
# combinations and most are null in both cases.
songplay_table_create = (""" 
CREATE TABLE IF NOT EXISTS songplays \
    (songplay_id SERIAL PRIMARY KEY, 
    start_time bigint NOT NULL, 
    user_id int NOT NULL, 
    level varchar NOT NULL,
    song_id varchar , 
    artist_id varchar , 
    session_id int NOT NULL, 
    location text NOT NULL, 
    user_agent text)
""")

user_table_create = (""" 
CREATE TABLE IF NOT EXISTS users 
    (user_id int PRIMARY KEY, 
    first_name varchar NOT NULL, 
    last_name varchar NOT NULL, 
    gender varchar(1), 
    level varchar NOT NULL)
""")

song_table_create = (""" 
CREATE TABLE IF NOT EXISTS songs 
    (song_id varchar PRIMARY KEY, 
    title text  NOT NULL, 
    artist_id varchar  NOT NULL, 
    year int, 
    duration numeric);
""")

artist_table_create = (""" 
CREATE TABLE IF NOT EXISTS artists 
    (artist_id varchar PRIMARY KEY, 
    name varchar  NOT NULL, 
    location text, 
    latitude decimal, 
    longitude decimal)
""")

time_table_create = (""" 
CREATE TABLE IF NOT EXISTS time 
    (start_time bigint PRIMARY KEY, 
    hour int  NOT NULL , 
    day int  NOT NULL, 
    week int  NOT NULL, 
    month int  NOT NULL, 
    year int  NOT NULL, 
    weekday varchar  NOT NULL)

""")

# INSERT RECORDS

songplay_table_insert = (""" 
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s) 
""")

user_table_insert = (""" 
INSERT INTO users (user_id, first_name, last_name, gender, level) 
    VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT (user_id) 
        DO UPDATE
        SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name,
        gender = EXCLUDED.gender, level = EXCLUDED.level
""")

song_table_insert = (""" 
INSERT INTO songs (song_id, title, artist_id, year, duration) 
    VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT (song_id) 
        DO UPDATE
        SET title = EXCLUDED.title, artist_id = EXCLUDED.artist_id,
        year = EXCLUDED.year, duration = EXCLUDED.duration 
""")

artist_table_insert = (""" 
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT (artist_id) 
        DO UPDATE
        SET name = EXCLUDED.name, location = EXCLUDED.location,
        latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude
""")


time_table_insert = (""" 
INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
    VALUES (%s, %s, %s, %s, %s, %s, %s) \
    ON CONFLICT (start_time) \
    DO NOTHING 
""")

# FIND SONGS

song_select = (""" 
SELECT S.song_id, A.artist_id FROM songs S 
    JOIN artists A ON S.artist_id = A.artist_id
    WHERE S.title = %s AND A.name = %s AND S.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop,
                      time_table_drop]