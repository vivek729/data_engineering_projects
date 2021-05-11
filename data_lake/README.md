# Data Lake

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As data engineer, we are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

We'll be able to test our database(eg. present as parquet files on S3) and ETL pipeline by running queries(eg. via Amazon Athena using AWS Glue) given by the analytics team from Sparkify and compare your results with their expected results.

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

- `dl.cfg.example`
	- Sample configuration file based on which `dl.cfg` to be created in same directory. This file contains
AWS access key ID and secret access key.

### Python scripts

- `etl.py`
    - Loads data from S3 into spark dataframes and writes them back to another S3 bucket after transforming them to fact and dimension tables. The tables are finally written in parquet format in S3.

## ETL Process

- Load song data and log data from S3 bucket using Spark into the spark dataframes. Transform these dataframes to create fact and dimension tables as per the schema described above. Ensure schema is correct(eg. column types). Finally the dataframes are written onto a S3 bucket in parquet format.

## Instructions

We can do the following in order to create our data lake:
- Create and launch an EMR cluster with a key-value pair associated to it.
  Recommend to use following settings under "edit software settings" while creating the cluster:
  
  ```
	[
	  {
	    "Classification": "spark",
	    "Properties": {
	    "maximizeResourceAllocation": "true"
	    }
	  }
	]
   ```
	
- Place the `etl.py` script and `dl.cfg` onto the EMR master node(/home/hadoop/sparkify/).

- Submit `etl.py`: `spark submit etl.py`

