
# Data Modeling with Postgres

## Introduction

Sparkify is a startup company which serves songs to its customers via its music streaming app. The company is interested in 
performing data analysis on the data collected by their music streaming app. However, the data resides in local directories
in json files and are not stored in database yet - making the task of analysis difficult in current situtaion.

Our goal as a data engineer is to create a Postgres database with tables designed to optimize queries on songplay analysis.
We need to create a database schema and ETL pipeline. The ETL pipeline will transfer data from files in two local directories
into tables in Postgres using Python and SQL.

## Schema Design

We will be using a star schema to organize our data into tables. 
We define following fact and dimension tables:

### Fact Table:

- __`songplays`__
    - Records in log data associated with song plays.
    - (`songplay_id`, `start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location`, `user_agent`)

### Dimension Tables:

- __`users`__
    - Users in the app
    - (`user_id`, `first_name`, `last_name`, `gender`, `level`)
    
- __`songs`__
    - Songs in music database
    - (`song_id`, `title`, `artist_id`, `year`, `duration`)

- __`artists`__
    - Artists in music database
    - (`artist_id`, `name`, `location`, `latitude`, `longitude`)
    
- __`time`__
    - Timestamps of records in __`songplays`__ broken down into specific units
    - (`start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`)
    
## Repository contents

### Data files

- All song data json files are present in `data/song_data`
- All log data json files are present in `data/log_data`

### Jupyter notebooks / Python scripts

- `test.ipynb`
    -  Displays the first few rows of each table to let us check our database.
- `create_tables.py`
    - Drops and creates our tables. We run this file to reset your tables before each time we run your ETL scripts.
- `etl.ipynb`
    - Reads and processes a single file from song_data and log_data and loads the data into our tables. This notebook contains detailed instructions on the ETL process for each of the tables.
- `etl.py`
    - Reads and processes files from song_data and log_data and loads them into our tables.
- `sql_queries.py`
    - Contains all our sql queries, and is imported into the last three files above.

## Instructions to create and test the Database

We can run the following in order to create and test our database:

- `create_tables`.py
    - This will create our database along with required tables
- `etl`.py
    - This will load all the data from json files into our fact and dimension tables created in above step.
- `test`.ipynb   
    - Displays the first few rows of each table to let us check our database.
