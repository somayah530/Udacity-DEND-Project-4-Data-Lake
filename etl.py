#!/usr/bin/python
# -*- coding: utf-8 -*-
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, \
    weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create spark session.

    Returns:
        spark (SparkSession) - spark session connected to AWS EMR cluster
    """

    spark = SparkSession.builder.config('spark.jars.packages',
            'org.apache.hadoop:hadoop-aws:2.8.5').getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function:
    - Load data from song_data dataset
    - Extract columns for songs and artist tables
    - Read song_data file
    - Write the data into parquet files
    
    ------
    spark: SparkSession 
    input_data: Path to the song_data s3 bucket
    output_data: Path to the written destination for parquet files
    
    """

    # get filepath to song data file

    song_data = os.path.join(input_data, 'song-data/A/A/A/*.json')

    # read song data file

    df = spark.read.json(song_data)

    # extract columns to create songs table

    songs_table = df['song_id', 'artist_id', 'title', 'year', 'duration'
                     ]
    songs_table = songs_table.dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist

    songs_table.write.partitionBy('year', 'artist_id'
                                  ).parquet(os.path.join(output_data,
            'songs.parquet'), 'overwrite')

    # extract columns to create artists table

    artists_table = df['artist_id', 'name', 'location',
                       'artist_latitude', 'artist_longitude']
    artists_table = artists_table.dropDuplicates(['artist_id'])

    # write artists table to parquet files

    artists_table.write.parquet(os.path.join(output_data,
                                'artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    This function:
    - Load data from log_data dataset
    - Extract columns for users and time tables
    - Read log_data file
    - Write the data into parquet files
    
    ------
    spark: SparkSession 
    input_data: Path to the log_data s3 bucket
    output_data: Path to the written destination for parquet files
    
    """

    # get filepath to log data file

    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file

    df = spark.read.json(log_data)

    # filter by actions for song plays

    songplays_table = df['ts', 'userId', 'level', 'sessionId',
                         'location', 'userAgent']

    # extract columns for users table

    users_table = df['userId', 'first_name', 'last_name', 'gender','level']
    users_table = users_table.dropDuplicates(['userId'])

    # write users table to parquet files

    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column

    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column

    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)
                       / 1000.0)))
    df = df.withColumn('datetime', get_datetime(df.ts))

    # extract columns to create time table

    time_table = df.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'),
        )
    time_table = time_table.dropDuplicates(['start_time'])

    # write time table to parquet files partitioned by year and month

    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,'time.parquet'), 'overwrite')
 
    # read in song data to use for songplays table

    song_data = os.path.join(input_data, 'song-data/A/A/A/*.json')
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table

    df = df.join(song_df, song_df.title == df.song)

    songplays_table = df.select(
        col('ts').alias('ts'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('ssessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        month('datetime').alias('month'),
        )

    songplays_table = songplays_table.selectExpr('ts as start_time')
    songplays_table.select(monotonically_increasing_id().alias('songplay_id'
                           )).collect()

    # write songplays table to parquet files partitioned by year and month

    songplays_table.write.partitionBy('year', 'month'
            ).parquet(os.path.join(output_data, 'songplays.parquet'),
                      'overwrite')


def main():
    """
    This function:
    - Call create_spark_session function 
    - process_song_data to ETL Songs data into Songs and Artists tables
    - process_log_data to ETL Users, Time, and Songplays tables
 
    """

    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://project4-dend/'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    main()
