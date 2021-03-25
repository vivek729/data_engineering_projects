# Cloud Data Warehouse

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, we are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. We'll be able to test your database and ETL pipeline by running queries given to us by the analytics team from Sparkify and compare our results with their expected results.

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

### Configuration files

- `dwh.cfg.example`
    - Sample configuration file based on which `dwh.cfg` to be created in same directory. This file contains
information like required database credentials and endpoint, role ARN, port of our Amazon Redshift cluster.

### Python scripts

- `create_table.py`
    - Drops and creates our tables - staging tables and final(analytics) tables. We run this file to reset your tables before each time we run your ETL scripts.
- `etl.py`
    - Loads data from S3 into staging tables on Redshift and then inserts records into our analytics tables on Redshift -  using these staging tables.
- `sql_queries.py`
    - Contains all our SQL queries, and is imported into the two files above.

## ETL Process

- Load staging tables - `events_stage` and `songs_stage` - on our Redshift cluster by copying JSON files located on a public S3 bucket.
- Insert into analytics tables using the above staging tables created.

## Instructions to create the database

We can do the following in order to create our database:
- Create and launch a Redshift Cluster
    - Associate a role to it so that it can read from S3.
    - Assign proper security group with required inbound rules so that it's accessible from public internet.
- Create `dwh.cfg`
    - Create file `dwh.cfg` based on the provided file `dwh.cfg.example`

-  Run `create_table`.py

- Run `etl`.py
