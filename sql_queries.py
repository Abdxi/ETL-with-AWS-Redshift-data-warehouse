"""
In this module we write all the nessary sql queties needed in the project.

module converts the most used queries into variables o ease queries usage.

"""

# create tables queries

import configparser


# import DWH parameters fro file
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))





#query of creating the fact table songplays
create_songplays = ("""
CREATE TABLE IF NOT EXISTS songplays
    (songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
    start_time bigint NOT NULL, 
    user_id int NOT NULL, 
    level varchar,
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location text, 
    user_agent text);

""")

create_users = ("""

CREATE TABLE IF NOT EXISTS users 
    (user_id int PRIMARY KEY, 
    first_name varchar, 
    last_name varchar, 
    gender varchar(1), 
    level varchar);
""")

create_songs = ("""

CREATE TABLE IF NOT EXISTS songs 
    (song_id varchar PRIMARY KEY, 
    title text, 
    artist_id varchar, 
    year int, 
    duration numeric);
""")

create_artist = ("""

CREATE TABLE IF NOT EXISTS artist

(artist_id varchar PRIMARY KEY, 
    name varchar, 
    location text, 
    latitude decimal, 
    longitude decimal);

""")

create_time = (""" 
CREATE TABLE IF NOT EXISTS time 
    (start_time timestamp PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday varchar);
""")

create_staging_events = (""" 
CREATE TABLE IF NOT EXISTS staging_events 
    (artist              VARCHAR,
    auth                VARCHAR,
    firstName           VARCHAR,
    gender              VARCHAR,
    itemInSession       INTEGER,
    lastName            VARCHAR,
    length              FLOAT,
    level               VARCHAR,
    location            VARCHAR,
    method              VARCHAR,
    page                VARCHAR,
    registration        FLOAT,
    sessionId           INTEGER,
    song                VARCHAR,
    status              INTEGER,
    ts                  TIMESTAMP,
    userAgent           VARCHAR,
    userId              INTEGER 
    )

""")

create_staging_songs = (""" 
CREATE TABLE IF NOT EXISTS staging_songs 
    (num_songs          INTEGER,
    artist_id           VARCHAR,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    year                INTEGER)

""")


# copy data from s3 files to staging tables

staging_events =("""

COPY staging_events 
FROM {data_bucket}
credentials 'aws_iam_role={role_arn}'
region 'us-west-2' format as JSON {log_json_path}
timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], 
            role_arn=config['IAM_ROLE']['ARN'], 
            log_json_path=config['S3']['LOG_JSONPATH'])





staging_songs =("""
COPY staging_songs 
FROM {data_bucket}
credentials 'aws_iam_role={role_arn}'
region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], 
            role_arn=config['IAM_ROLE']['ARN'])



# insert data into each table

insert_songplays = ("""

INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT  DISTINCT(e.ts)  AS start_time, 
            e.userId        AS user_id, 
            e.level         AS level, 
            s.song_id       AS song_id, 
            s.artist_id     AS artist_id, 
            e.sessionId     AS session_id, 
            e.location      AS location, 
            e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  =  'NextSong'

""")


insert_users = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level) 
SELECT  DISTINCT(userId)    AS user_id,
        firstName           AS first_name,
        lastName            AS last_name,
        gender,
        level
FROM staging_events
WHERE user_id IS NOT NULL
AND page  =  'NextSong'

""")

insert_songs = ("""

INSERT INTO songs(song_id, title, artist_id, year, duration) 
SELECT  DISTINCT(song_id) AS song_id,
        title,
        artist_id,
        year,
        duration
FROM staging_songs
WHERE song_id IS NOT NULL

""")

insert_artist =("""
INSERT INTO artist(artist_id, name, location, latitude, longitude) 
SELECT  DISTINCT(artist_id) AS artist_id,
        artist_name         AS name,
        artist_location     AS location,
        artist_latitude     AS latitude,
        artist_longitude    AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL


""")

insert_time = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
    
    SELECT  DISTINCT(start_time)                AS start_time,
            EXTRACT(hour FROM start_time)       AS hour,
            EXTRACT(day FROM start_time)        AS day,
            EXTRACT(week FROM start_time)       AS week,
            EXTRACT(month FROM start_time)      AS month,
            EXTRACT(year FROM start_time)       AS year,
            EXTRACT(dayofweek FROM start_time)  as weekday
    FROM songplays


""")


select_ids = ("""

SELECT artist.artist_id, songs.song_id

FROM songs

JOIN artist 

ON songs.artist_id = artist.artist_id

WHERE songs.title = %s AND artist.name = %s  AND songs.duration = %s


 """)

# drop queries for each table

drop_songplays = "DROP TABLE IF EXISTS songplays"
drop_songs = "DROP TABLE IF EXISTS songs"
drop_artist = "DROP TABLE IF EXISTS artist"
drop_users = "DROP TABLE IF EXISTS users"
drop_time = "DROP TABLE IF EXISTS time"
drop_staging_events = "DROP TABLE IF EXISTS staging_events;"
drop_staging_songs = "DROP TABLE IF EXISTS staging_songs;"



# group create tables in one list

create_queries = [create_songplays, create_users, create_songs, create_artist, create_time]

# group staging tables in one list

staging_queries = [staging_songs, staging_events]

# group drop table in one list

drop_queries = [drop_songplays, drop_users, drop_songs, drop_artist, drop_time]

# group drop table in one list

insert_queries = [insert_songplays, insert_users, insert_songs, insert_artist, insert_time]



