import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, row_number
import pandas as pd


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config["AWS"]['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config["AWS"]['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a Spark session to be used for data processing.

    Returns:
        spark (SparkSession): An active Spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes song data files, extracts relevant columns to create songs and artists tables,
    and writes these tables to parquet files.

    Args:
        spark (SparkSession): The active Spark session.
        input_data (str): The input data path location on S3.
        output_data (str): The output data path location on S3.

    Returns:
        None
    """
    # get filepath to song data file
    song_data = f"{input_data}/song_data/*/*/*/"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.parquet(f"{output_data}/songs/", partitionBy=["year", "artist_id"])

    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}/artists/")


def process_log_data(spark, input_data, output_data):
    """
    Processs log data files, extracts relevant columns to create users, time, and songplays tables,
    and writes these tables to parquet files.

    Args:
        spark (SparkSession): The active Spark session.
        input_data (str): The input data path location on S3.
        output_data (str): The output data path location on S3.

    Returns:
        None
    """

    # get filepath to log data file
    log_data = f"{input_data}/log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId","firstName","lastName","gender","level").dropDuplicates(["userId"])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users/"))

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0))
    df = df.withColumn("start_time", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",date_format("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time_table/"), partitionBy=["year","month"])

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    song_log_joined_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), how='inner')
    
    songplays_table = song_log_joined_table.select("userId", "timestamp", "song_id", "artist_id", "level", "sessionId", "location", "userAgent" ) \
                        .withColumn("songplay_id", row_number().over( Window.partitionBy('timestamp').orderBy("timestamp"))) \
                        .withColumnRenamed("userId","user_id")        \
                        .withColumnRenamed("timestamp","start_time")  \
                        .withColumnRenamed("sessionId","session_id")  \
                        .withColumnRenamed("userAgent", "user_agent")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays/"), partitionBy= ["year", "month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-output/" 
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
