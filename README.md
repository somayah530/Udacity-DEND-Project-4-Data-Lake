
## Udacity Data Engineer Nano Degree Project-4

## Data Lake with Apache Spark

# 1.Overview

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project we were tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


#  2.Project Steps
<ul>

### Reads data from S3, processes that data using Spark, and writes them back to S3 

*  Add AWS IAM Credentials in `dl.cfg`

*  Specify desired output data path in the main function of `etl.py` 

*  Execute the ETL pipeline script by running "python etl.py"


### Database schemas for fact and dimension tables


* Fact Table: (songplays)
  Is the central table in a star schema of a data warehouse. A fact table stores quantitative information for analysis and is often denormalized.

* Dimension Tables:(song, user, time, artist)
  They used to describe dimensions; they contain dimension keys, values and attributes.


![Database ER Diagram4](https://user-images.githubusercontent.com/24846149/91642215-2f3cce80-ea32-11ea-9c34-ca11145090fd.png)





#### References

 Amazon S3 - https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html



