# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level level_enum NOT NULL,
        song_id VARCHAR(30),
        artist_id VARCHAR(30),
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
        gender gender_enum NOT NULL,
        level level_enum NOT NULL
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
        artist_id VARCHAR(30) PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
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

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (songplay_id)
    DO NOTHING;
""")

user_table_insert = ("""
    INSERT INTO users VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
        DO UPDATE
        SET level = EXCLUDED.level;
""")

song_table_insert = ("""
    INSERT INTO songs VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
    DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT s.song_id, a.artist_id
    FROM songs AS s INNER JOIN artists AS a ON s.artist_id = a.artist_id
    WHERE s.title = %s AND a.name = %s AND s.duration = %s;
""")

# create enum data types

level_enum_create = ("""
    CREATE TYPE level_enum AS ENUM (
        'free', 'paid'
    );
""")

gender_enum_create = ("""
    CREATE TYPE gender_enum AS ENUM (
        'M', 'F'
    );
""")

# drop enum data types

level_enum_drop = ("""
    DROP TYPE IF EXISTS level_enum;
""")

gender_enum_drop = ("""
    DROP TYPE IF EXISTS gender_enum;
""")


# QUERY LISTS

create_enumerated_type_queries = [level_enum_create, gender_enum_create]
drop_enumerated_type_queries = [level_enum_drop, gender_enum_drop]
create_table_queries = [artist_table_create, song_table_create, user_table_create, time_table_create, songplay_table_create]
drop_table_queries = [artist_table_drop, song_table_drop, user_table_drop, songplay_table_drop, time_table_drop]
