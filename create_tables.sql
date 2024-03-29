#drop tables if existing
DROP TABLE IF EXISTS movie_info_staging
DROP TABLE IF EXISTS cast_staging
DROP TABLE IF EXISTS cast_movie_rel_staging
DROP TABLE IF EXISTS reviewer_staging
DROP TABLE IF EXISTS movie_review_staging

DROP TABLE IF EXISTS movie_info_tables
DROP TABLE IF EXISTS cast_table
DROP TABLE IF EXISTS cast_movie_rel_table
DROP TABLE IF EXISTS reviewer_table
DROP TABLE IF EXISTS movie_review_table

#create Staging tables
CREATE TABLE IF NOT EXISTS movie_info_staging (movie_id varchar, 
                                             movie_title varchar, 
                                             movie_release_year int, 
                                             runtimeMinutes int, 
                                             movie_type varchar,
                                             genres varchar);


CREATE TABLE IF NOT EXISTS cast_table_staging (cast_id varchar, 
                                       cast_name varchar, 
                                       birth_year int, 
                                       death_year int, 
                                       primary_Profession varchar);
                                       

CREATE TABLE IF NOT EXISTS cast_movie_rel_staging (cast_id varchar, 
                                                   knownForTitles varchar);
 
 
CREATE TABLE IF NOT EXISTS reviewer_staging (user_id varchar, 
                                       screen_name varchar, 
                                       location int);
                   
CREATE TABLE IF NOT EXISTS movie_review_staging (movie_id varchar,
	                                               user_id varchar,
	                                               review_text text,
	                                               retweet_count int,
	                                               favorite_count INT,
	                                               review_score numeric,
	                                               date_created date)


#create final tables
CREATE TABLE IF NOT EXISTS movie_info_table (movie_id varchar PRIMARY KEY, 
                                             movie_title varchar NOT NULL, 
                                             movie_release_year int NOT NULL, 
                                             runtimeMinutes int NOT NULL, 
                                             movie_type varchar NOT NULL,
                                             genres varchar);


CREATE TABLE IF NOT EXISTS cast_table (cast_id varchar PRIMARY KEY, 
                                       cast_name varchar NOT NULL, 
                                       birth_year int NOT NULL, 
                                       death_year int NULL, 
                                       primary_Profession varchar NOT NULL);
                                       

CREATE TABLE IF NOT EXISTS cast_movie_rel (cast_id varchar NOT NULL, 
						                                       knownForTitles varchar NOT NULL
						                                       CONSTRAINT cast_title_rel UNIQUE(cast_id,knownForTitles));
						 
 
CREATE TABLE IF NOT EXISTS reviewer_table (user_id varchar PRIMARY KEY, 
		                                       screen_name varchar NOT NULL, 
		                                       location int NULL);
                   
CREATE TABLE IF NOT EXISTS movie_review_table (movie_id varchar NOT NULL,
                                               user_id varchar NOT NULL,
                                               review_text text NOT NULL,
                                               retweet_count int NULL,
                                               favorite_count INT NULL,
                                               review_score numeric NOT NULL ,
                                               date_created date NOT NULL,
                                               primary key(movie_id, user_id,date_created))