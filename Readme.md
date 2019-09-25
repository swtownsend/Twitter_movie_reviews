# Udacity Capstone Project
## Social media movie review database

### Purpose
This project will search twitter for reviews to movies from the IMDB data sets and give a sentiment score based on the review.
Sites like rotten tomatoes review are often binary, either a review is either fresh(good) or rotten(bad). 
Most people understand that there is more than one way to feel about a movie the n just good or bad and this analysis of reviews will allow us to see a more nuanced view of how people feel about certain movies.
The IMDB datasets will be put into the movie_info_table,cast_table, and the cast_movie_rel_table. Any records associated to a tv shows or episodes will be removed.
The JSON data from the TWITTER api class will be parsed to get information on the person how reviewed the movie and then all these tables will be connected to the movie_review_table.  

### Schema
The data will be in star schema to for analysis.
the dimension tables will consist of movie_info_table,cast_table, the cast_movie_rel_table and the reviewer table.
the fact table is the movie_review_table.
the reviewer and the movie_info_table will have a relationship to the movie_review table, 
while the cast able will be have a many to many relationships to the movie_info table by the cast_movie_rel_table.

### Data
The movie data will come the IMDB sample data that can be 
downloaded from https://datasets.imdbws.com/
datasets used:
name.basics.tsv
title.basics.tsv
Twitter search API

### Technology

The datalake will be running on a AWS EMR SPARK cluster.  This will allow for distributed processing of the data from 
TWITTER and will allow me to scale the application and increase the performance of getting the information form the TWIITER API.

The information from movies and review will not be changing often the data will be written to parquet files that will be stored on an s3 drive. This will help with storage and increased read speed for the analytics team.

The data will be updated on a weekly basis after the weekend to match new movie releases.

#### Additional Python packages
For this etl top run correctly the following python packages need to installed onthe system.
Tweepy for getting the tweets from TWITTER
textblob to do the sentiment analysis
configparser to read the config files.

### Analytics

Sample queries and analytics of the data will show actual public sentiment from movie audiences.  
Which review have the most retweets and favorites.
Also, retrieve a list of review that match sentiment levels.


### Scenarios 

1. If the data was increased by 100x. 
	 The schema and data is already scalable for increase data amounts and I would increase the spark cluster to compensate for the increased processing ting

2. If the pipelines were run daily by 7am.
	 
	 
3. If the database needed to be accessed by 100+ people
	 With many people accessing the data, Integrity becomes an issue.  The parquet files would have to be marked as read only 
	 except when the datalake processing happens.  this will ensure that no-one overwrites the data that is already in the datalake.
  

