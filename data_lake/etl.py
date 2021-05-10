import configparser
import datetime as dt
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (year, month, dayofmonth, hour,
                                   weekofyear, dayofweek, date_format)
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a spark session.

    Returns:
        SparkSession: SparkSession object.
    """
    spark = (SparkSession.builder
                         .config("spark.jars.packages",
                                 "org.apache.hadoop:hadoop-aws:3.2.1")
                         .getOrCreate())
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads song data from S3 bucket in a dataframe that is used to create
    `songs` and `artists` dataframes and written to another S3 bucket in
    parquet format.

    Args:
        spark (SparkSession): SparkSession object
        input_data (str): S3 bucket path where song data is located
        output_data (str): S3 bucket path where `songs` and `artists` table to be
                           written in parquet format.
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    # read song data file
    song_data_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table_df = song_data_df.select("song_id", "title", "artist_id",
                                         "year", "duration")

    # write songs table to parquet files partitioned by year and artist
    songs_output_path = os.path.join(output_data,
                                     "sparkify/songs/songs_table.parquet")

    songs_table_df.write.partitionBy("year", "artist_id") \
                        .mode("overwrite") \
                        .parquet(songs_output_path)

    # extract columns to create artists table
    artists_table_df = song_data_df.select("artist_id", "artist_name",
                                           "artist_location", "artist_latitude",
                                           "artist_longitude")

    artists_table_unique_df = artists_table_df.dropDuplicates()

    # write artists table to parquet files
    artists_output_path = os.path.join(output_data,
                                       "sparkify/artists/artists_table.parquet")

    artists_table_unique_df.write.mode("overwrite").parquet(artists_output_path)


def process_log_data(spark, input_data, output_data):
    """
    Reads log data from S3 bucket in a dataframe that is used to create `users`,
    `time` and `songplays` dataframes and written to another S3 bucket in
    parquet format.

    Args:
        spark (SparkSession): SparkSession object
        input_data (str): S3 bucket path where log data is located
        output_data (str): S3 bucket path where `users`, `time` and `songplays`
                           table to be written in parquet format.
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    logs_df = spark.read.json(log_data)

    # filter by actions for song plays
    logs_filtered_df = logs_df.where(logs_df.page == "NextSong")

    # extract columns for users table
    users_table_df = logs_filtered_df.selectExpr("userId AS user_id",
                                                 "firstName AS first_name",
                                                 "lastName AS last_name",
                                                 "gender",
                                                 "level"
                                                 )
    users_table_unique_df = users_table_df.dropDuplicates()

    # write users table to parquet files
    users_output_path = os.path.join(output_data,
                                     "sparkify/users/users_table.parquet")
    users_table_unique_df.write.mode("overwrite").parquet(users_output_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: dt.datetime.utcfromtimestamp(x / 1000),
                        TimestampType())
    logs_filtered_df = logs_filtered_df.withColumn("start_time",
                                                   get_timestamp("ts"))

    # extract columns to create time table
    time_table_df = logs_filtered_df.select("start_time")
    time_table_unique_df = time_table_df.dropDuplicates()
    time_table_final_df = time_table_unique_df.select(
        "start_time",
        hour("start_time").alias("hour"),
        dayofmonth("start_time").alias("day"),
        weekofyear("start_time").alias("week"),
        month("start_time").alias("month"),
        year("start_time").alias("year"),
        dayofweek("start_time").alias("weekday")
    )

    # write time table to parquet files partitioned by year and month
    time_output_path = os.path.join(output_data,
                                    "sparkify/time/time_table.parquet")
    time_table_final_df.write \
                       .partitionBy("year", "month") \
                       .mode("overwrite") \
                       .parquet(time_output_path)

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    song_data_df = spark.read.json(song_data)

    # Create `songplays` table by joining `logs_filtered_df` and `song_data_df`
    logs_filtered_df.createOrReplaceTempView("logs_filtered_table")
    song_data_df.createOrReplaceTempView("song_data_table")

    songplays_table_df = spark.sql("""
        SELECT
            row_number() OVER(ORDER BY start_time) AS songplay_id,
            l.start_time,
            l.userId AS user_id,
            l.level,
            s.song_id,
            s.artist_id,
            l.sessionId AS session_id,
            s.artist_location,
            l.userAgent AS user_agent
        FROM logs_filtered_table AS l
        LEFT OUTER JOIN song_data_table AS s ON
            (l.song = s.title)
            AND (l.artist = s.artist_name)
            AND (l.length = s.duration)
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table_df = songplays_table_df.withColumn("year", year("start_time")) \
                                           .withColumn("month", month("start_time"))
    songplays_output_path = os.path.join(output_data,
                                         "sparkify/songplays/songplays_table.parquet")
    songplays_table_df.write.partitionBy("year", "month") \
                      .mode("overwrite") \
                      .parquet(songplays_output_path)


def main():
    """
    Calls functions to create spark session, process song and log data.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sbucket62/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
