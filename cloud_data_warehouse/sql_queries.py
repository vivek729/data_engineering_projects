import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_stage;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_stage;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS events_stage (
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender VARCHAR,
        item_in_session INTEGER,
        last_name VARCHAR,
        length DECIMAL,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration DECIMAL,
        session_id INTEGER,
        song VARCHAR,
        status INTEGER,
        ts TIMESTAMP,
        user_agent VARCHAR,
        user_id INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs_stage (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude DECIMAL,
        artist_longitude DECIMAL,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration DECIMAL,
        year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0, 1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL SORTKEY,
        user_id INTEGER NOT NULL,
        level VARCHAR(4) NOT NULL,
        song_id VARCHAR(30),
        artist_id VARCHAR(30) DISTKEY,
        session_id INTEGER NOT NULL,
        location VARCHAR(200) NOT NULL,
        user_agent VARCHAR(200) NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users(user_id),
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
        FOREIGN KEY (song_id) REFERENCES songs(song_id),
        FOREIGN KEY (start_time) REFERENCES time(start_time)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        gender VARCHAR(1) NOT NULL,
        level VARCHAR(4) NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(30) PRIMARY KEY,
        title VARCHAR(100) NOT NULL,
        artist_id VARCHAR(30),
        year INTEGER NOT NULL,
        duration DECIMAL NOT NULL,
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(30) PRIMARY KEY DISTKEY,
        name VARCHAR(200) NOT NULL,
        location VARCHAR(200) NOT NULL,
        latitude DECIMAL,
        longitude DECIMAL
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday INTEGER NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY events_stage
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {} 
    REGION 'us-west-2'
    TIMEFORMAT as 'epochmillisecs';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY songs_stage
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto'
    REGION 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT e.ts::date,
           e.user_id,
           e.level,
           s.song_id,
           s.artist_id,
           e.session_id,
           s.artist_location,
           e.user_agent
    FROM events_stage AS e
    INNER JOIN songs_stage AS s ON (e.song = s.title) AND (e.artist = s.artist_name) AND (e.length = s.duration);
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM events_stage
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM songs_stage;
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM songs_stage;
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT DISTINCT ts::date AS s, 
                    EXTRACT (HOUR from s),
                    EXTRACT (DAY from s),
                    EXTRACT (WEEK from s),
                    EXTRACT (MONTH from s),
                    EXTRACT (YEAR from s),
                    EXTRACT (WEEKDAY from s)
    FROM events_stage;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, artist_table_create, song_table_create, user_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, song_table_drop, artist_table_drop, user_table_drop, time_table_drop]
# drop_table_queries = [songplay_table_drop, song_table_drop, artist_table_drop, user_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [artist_table_insert, song_table_insert, user_table_insert, time_table_insert, songplay_table_insert]
