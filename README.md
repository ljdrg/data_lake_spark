# Data Lake Project

The purpose of this project is to help the startup Sparkify to analyse their data in a distirubted way. As they were grwoing quickly they needed a good solution for the ever growing incoming data.

## Input Data

The input data is stored in an S3 bucket. It contains two datasets, data about the songs and log data.

The song dataset consists of various JSON files. They are partitioned by the first three letters of each song's track ID.

Examples:
`song_data/A/B/C/TRABCEI128F424C983.json`
`song_data/A/A/B/TRAABJL12903CDCF1A.json`

One file contains the following information: 

`{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`

The log data consists also of JSON files. The logs are partitioned by year and month. 
A file contains the following information: 
- artist
- auth
- firstName
- gender 
- itemInSession
- lastName
- length
- level
- location
- method
- page
- registration
- sessionId
- song
- status 
- ts
- userAgent
- userId

# Output Schema

The output schema is a star schema consisting of one fact and four dimension tables. 

- songplays: a fact table, containing information about what song was played when and by which user
- users: dim table, containing information about the sparkify user
- songs: dim table, containing information about the songs like title, length
- artists: dim table, containing information about the artists like name and location
- time: dim table, extracting information from the timestamp when a song was played like day and month

# ETL Pipeline

The ETL pipeline 

1. loads the data from the S3 bucket `S3a://udacity-dend/` into Spark dataframes
2. processes the data into the described schema
3. writes the processed data back into an output S3 bucket `S3a://udacity-dend-output`

# ToDos:
1. Create an AWS IAM role with S3 read access
2. Enter IAM credentials in the dl.cfg file
3. create an S3 bucket for the output tables
4. Run python etl.py