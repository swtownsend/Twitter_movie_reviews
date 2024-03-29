#import the libaries need to read the config file
#start and run the spark session and process the  files
import configparser
from TwitterClient import TwitterClient
import pandas as pd
import os
from pprint import pformat
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import split,explode
from pyspark.sql.functions import udf, col,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from insert_tables import insert_table_queries, truncate_staging

# access and read the config file
config = configparser.ConfigParser()
config.read('capstone.cfg')


#access the the AWS key_id and the secret acsess key fromthe config file 
# this willallow the program to read and write to the amazon S3 buckets
os.environ['AWS_ACCESS_KEY_ID']=config.get('USER','AWS_ACCESS_KEY_ID')#config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('USER','AWS_SECRET_ACCESS_KEY')#config['AWS_SECRET_ACCESS_KEY']


# create the spark session on the EMR cluster
def create_spark_session():
    spark = SparkSession.builder.\
        config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
    return spark


#Function to  process the song JSON song data to create the songs and artists tables
def process_title_data(spark, input_data, output_data):
    # get filepath to title data file
    movie_data = os.path.join(input_data, "title/*.tsv")
    sqlContext = SQLContext(spark)  
    
    
    # read movie data file from the titles file
    global title_df 
    title_df = sqlContext.read.format('csv').option("delimiter", "\t").option("header", "true").load(movie_data).dropDuplicates()
    
    # extract columns to create movie table
    global movie_info_table
    movie_info_table = \
    title_df.select('tconst','primaryTitle','startYear','runtimeMinutes','titleType','genres')
    
    #filter out tv movies and tv shows
    movie_info_table = movie_info_table.filter(((movie_info_table.titleType == 'short') | (movie_info_table.titleType == 'movie')))
    
    #rename colums
    movie_info_table = \
    movie_info_table.toDF('movie_id','movie_title','movie_release_year','runtimeMinutes','movie_type','genres')
    
    # write title table to staging table in reshift 
    #movie_info_table.write.partitionBy('movie_release_year').parquet(f'{output_data}movie_info_table', mode='overwrite')
    movie_info_table.write \
    .format("com.databricks.spark.redshift") \
    .option("url", "jdbc:redshift://redshifthost:5439/{}?user={}&password={}" \
    .format(config.get('DATABASE','DB_NAME'),config.get('DATABASE','DB_USER'),config.get('DATABASE','DB_PASSWORD')) \
    .option("dbtable", "movie_info_staging") \
    .option("tempdir", "s3a:///swtown-capstone-udacity/output/") \
    .mode("error") \
    .save()



#Function to  process the song JSON log data to create the cast table
def process_name_data(spark, input_data, output_data):
    # get filepath to log data file
    name_data = os.path.join(input_data, "name/*.tsv")
    sqlContext = SQLContext(spark)  
    # read name data file
    cast_df = sqlContext.read.format('csv').option("delimiter", "\t").option("header", "true").load(name_data).dropDuplicates() 
    
    # extract columns for cast table    
    cast_table = \
    cast_df.select('nconst','primaryName','birthYear','deathYear','primaryProfession')  
    
    cast_table = \
    cast_table.toDF('cast_id','cast_name','birth_Year','death_Year','primary_Profession')  
       
    # write users table to redshift
    cast_table.write \
    .format("com.databricks.spark.redshift") \
    .option("url", "jdbc:redshift://redshifthost:5439/{}?user={}&password={}" \
    .format(config.get('DATABASE','DB_NAME'),config.get('DATABASE','DB_USER'),config.get('DATABASE','DB_PASSWORD')) \
    .option("dbtable", "cast_staging") \
    .option("tempdir", "s3a:///swtown-capstone-udacity/output/a") \
    .mode("error") \
    .save()
    
    #make many to many realtion model for cast and movies
    cast_movie_rel_table = cast_df.select('nconst','knownForTitles').where(col("dt_mvmt").isNotNull())
    
    
    #convert sting to array
    cast_movie_rel_table = cast_movie_rel_table.withColumn("knownForTitles",split(col("knownForTitles"), ",\s*"))
          
    #convert array to rows
    cast_movie_rel_table = cast_movie_rel_table.withColumn("knownForTitles", explode("knownForTitles"))
    #convert column to strings
    cast_movie_rel_table = cast_movie_rel_table.withColumn("knownForTitles", cast_movie_rel_table["knownForTitles"].cast(StringType()))
    
    #join move dat frame with cast movie table and filter movie from 
    #the  cast_movie_rel_table  that is not in the movie table
    cast_movie_rel_table.join(movie_info_table,[cast_movie_rel_table.knownForTitles == movie_info_table.movie_id],"leftanti")
    
    #filter row that are not in the 
    #write table to tile
    cast_movie_rel_table.write \
    .format("com.databricks.spark.redshift") \
    .option("url", "jdbc:redshift://redshifthost:5439/{}?user={}&password={}" \
    .format(config.get('DATABASE','DB_NAME'),config.get('DATABASE','DB_USER'),config.get('DATABASE','DB_PASSWORD')) \
    .option("dbtable", "cast_movie_rel_staging") \
    .option("tempdir", "s3a:///swtown-capstone-udacity/output/") \
    .mode("error") \
    .save()


def process_review_data(spark, output_data):
    
    #create conncetin to Twitter
    api = TwitterClient()
    #convert datframe colum movie title to list
    movie_titles = movie_info_table.select('movie_title').collect()
    
    #get the tweets for the movie title limte to the 1st 50
    tweets = api.get_tweets(movie_titles)
    #print('tweets: ',tweets)
        
    try:
    	#create review data frame
    	review_df = pd.DataFrame.from_dict(tweets)
    	review_df = spark.createDataFrame(tweets)

    	#select the fields for the review table
    	reviewer_table = \
    	review_df.select('user_id','screen_name','location').dropDuplicates()

    	#write the table to a parquet file
    	review_df.write \
    	.format("com.databricks.spark.redshift") \
    	.option("url", "jdbc:redshift://redshifthost:5439/{}?user={}&password={}" \
    	.format(config.get('DATABASE','DB_NAME'),config.get('DATABASE','DB_USER'),config.get('DATABASE','DB_PASSWORD')) \
    	.option("dbtable", "reviewer_staging") \
    	.option("tempdir", "s3a:///swtown-capstone-udacity/output/") \
    	.mode("error") \
    	.save()

    	#join the log and song dataframes 
    	movie_review_table = review_df.join(title_df,review_df.movie_title == title_df.primaryTitle)
    	
    	movie_review_table =  \
    	movie_review_table.select('movie_title',    
                                'tconst',
                                'user_id',
                                'text',
                                'retweet_count',
                                'favorite_count',
                                'sentiment',
                                'created_at',
                                month('created_at').alias('month'),
                                year('created_at').alias('year')
                                )

    	#rename columns for
    	movie_review_table.toDF('movie_id',
                              'user_id',
                              'review_text',
                              'retweet_count',
                              'favorite_count',
                              'review_score',
                              'date_created',
                              'month',
                              'year')

    	# write songplays table to parquet files partitioned by year and month
    	cast_movie_rel_table.write \
    	.format("com.databricks.spark.redshift") \
    	.option("url", "jdbc:redshift://redshifthost:5439/{}?user={}&password={}" \
    	.format(config.get('DATABASE','DB_NAME'),config.get('DATABASE','DB_USER'),config.get('DATABASE','DB_PASSWORD')) \
    	.option("dbtable", "movie_review_staging") \
    	.option("tempdir", "s3a:///swtown-capstone-udacity/output/") \
    	.mode("error") \
    	.save()
    	
    except Exception as e:
    	print(e)
            

# function move data from staing tables to 
def run_sql(cur, conn,table_list):
    for query in table_list:
        cur.execute(query)
        conn.commit()
        

# create the spark session and call the functions
# that will process the JSON files to the star shcema
# parquet files.
def main():
    #create spark session
    spark = create_spark_session()
    input_data = "s3a:///swtown-capstone-udacity/input/"
    
    #create database connection and cursor
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    process_title_data(spark, input_data, output_data)    
    process_name_data(spark, input_data, output_data)
    process_review_data(spark, output_data)
    run_sql(cur, conn,insert_table_queries)
    run_sql(cur, conn,truncate_staging)



if __name__ == "__main__":
    main()
