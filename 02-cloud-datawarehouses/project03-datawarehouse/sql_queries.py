import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events;"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs;"
songplay_table_drop = "DROP table IF EXISTS songplay;"
user_table_drop = "DROP table IF EXISTS user;"
song_table_drop = "DROP table IF EXISTS song;"
artist_table_drop = "DROP table IF EXISTS artist;"
time_table_drop = "DROP table IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events
(
  num_songs INTEGER,
  artist_id varchar,
  artist_latitude decimal,
  artist_longitude decimal,
  artist_location varchar,
  artist_name varchar,
  song_id varchar,
  title varchar,
  duration decimal,
  year int
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs
(
  artist varchar,
  auth varchar,
  firs_name varchar,
  gender varchar(1),
  item_session int,
  last_name varchar,
  length decimal,
  level varchar,
  location varchar,
  method varchar,
  page varchar,
  registration real,
  session_id bigint,
  song varchar,
  status int,
  ts bigint,
  user_agent varchar,
  user_id bigint
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays \
    (songplay_id bigint IDENTITY(0,1) PRIMARY KEY, 
    start_time bigint NOT NULL, 
    user_id int NOT NULL, 
    level varchar,
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent text)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user 
    (user_id int PRIMARY KEY NOT NULL, 
    first_name varchar, 
    last_name varchar, 
    gender varchar(1), 
    level varchar)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song 
    (song_id varchar PRIMARY KEY NOT NULL, 
    title varchar, 
    artist_id varchar, 
    year int, 
    duration decimal);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist 
    (artist_id varchar PRIMARY KEY NOT NULL, 
    name varchar, 
    location varchar, 
    latitude decimal, 
    longitude decimal)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
    (start_time bigint PRIMARY KEY NOT NULL, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday varchar)
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES


songplay_table_insert = ("""

insert into songplay
(select start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
from staging_songs;

""")

user_table_insert = ("""
begin transaction;

create temp table stage_user (like user); 

insert into stage_user 
select distinct user_id, first_name, last_name, gender, level from staging_songs;

update user
set first_name = stage_user.first_name, 
last_name = stage_user.last_name, 
gender = stage_user.gender 
level = stage_user.level
from stage_user
where user.user_id = stage_user.user_id; 

delete from stage_user 
using user 
where stage_user.user_id = user.user_id;

insert into user
(select user_id, first_name, last_name, gender, level)
from stage_user ;

drop table stage_user;

end transaction; 
""")


song_table_insert = ("""

begin transaction;

create temp table stage_song (like song); 

insert into stage_song 
select distinct song_id, title, artist_id, year, duration from staging_events;

update song
set title = stage_song.title, 
artist_id = stage_song.artist_id, 
year = stage_song.year,
duration = stage_song.duration
from stage_song
where song.song_id = stage_song.song_id; 

delete from stage_song 
using song
where stage_song.song_id = song.song_id;

insert into song
(select song_id, title, artist_id, year, duration)
from stage_song ;

drop table stage_song;

end transaction; 

""")

artist_table_insert = ("""

begin transaction;

create temp table stage_artist (like artist); 

insert into stage_artist 
select distinct artist_id, artist_name as name, artist_location as location, artist_latitude as latitude,
       artist_longitude as longitude from staging_events;

update artist
set name = stage_artist.name, 
location = stage_artist.location, 
latitude = stage_artist.latitude,
longitude = stage_artist.longitude
from stage_artist
where artist.artist_id = stage_artist.artist_id; 

delete from stage_artist 
using artist
where stage_song.song_id = song.song_id;

insert into artist
(select artist_id, name, location, latitude, longitude)
from stage_artist;

drop table stage_artist;

end transaction; 

""")

time_table_insert = ("""
begin transaction;

create temp table stage_time (like time); 

insert into stage_time 
select distinct ts start_time,
    extract(hour from ('1970-01-01'::date + ts/1000 * interval '1 second')) as hour,
    extract(day from ('1970-01-01'::date + ts/1000 * interval '1 second')) as day,   
    extract(week from ('1970-01-01'::date + ts/1000 * interval '1 second')) as week,
    extract(month from ('1970-01-01'::date + ts/1000 * interval '1 second')) as month,
    extract(year from ('1970-01-01'::date + ts/1000 * interval '1 second')) as year,
    extract(weekday from ('1970-01-01'::date + ts/1000 * interval '1 second')) as weekday   
from staging_songs

delete from stage_time 
using time
where stage_time.start_time = time.start_time;

insert into time
(select start_time, hour, day, week, month, year, weekday)
from stage_time;

drop table stage_time;

end transaction; 


""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
