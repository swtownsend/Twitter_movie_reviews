#insert into postgres tables

movie_info_table_insert = '''INSERT INTO movie_info_table (movie_id,movie_title,movie_release_year,runtimeMinutes,movie_type,genres)
														SELECT movie_id,movie_title,movie_release_year,runtimeMinutes,movie_type,genres
														FROM movie_info_staging
														WHERE movie_id IS NOT NULL
														ON CONFLICT (movie_id)
														DO NOTHING;'''


cast_table_insert = '''INSERT INTO cast_table (cast_id,cast_name,birth_year,death_year,primary_Profession)
											SELECT cast_id,cast_name,birth_year,death_year,primary_Profession   
											FROM cast_staging
											WHERE cast_id IS NOT NULL
											ON CONFLCIT (cast_id)
											DO UPDATE cast_name = EXCLUDED.cast_name,birth_year = EXCLUDED.birth_year,
											death_year = EXCLUDED.death_year,primary_Profession = EXCLUDED.primary_Profession ;'''                                    

cast_movie_rel_table_insert = '''INSERT INTO cast_movie_rel_table (cast_id,knownForTitles);
																SELECT cast_id,knownForTitles  
																FROM cast_movie_rel_staging
																WHERE cast_id IS NOT NULL
																ON CONSTRAINT
																DO NOTHING;''' 
 
reviewer_table_insert = '''INSERT INTO reviewer_table (user_id,screen_name,location);
														SELECT user_id,screen_name,location  
														FROM reviewer_staging
														WHERE user_id IS NOT NULL
														ON CONFLICT (user_id)
														DO NOTHING;'''
                   
movie_review_table = '''INSERT INTO movie_review_table 
                        (movie_id,user_id,review_text,retweet_count,favorite_count,review_score,date_created)
												SELECT movie_review_id,movie_id,user_id,review_text,retweet_count,favorite_count,review_score,date_created  
												FROM movie_review_staging
												WHERE movie_review_id IS NOT NULL
												ON CONFLICT (movie_id, user_id,date_created)
												DO NOTHING;'''
												

#truncate staging tables
truncate_movie_info_staging = '''TRUNCATE movie_info_staging'''
truncate_cast_staging = '''TRUNCATE cast_staging'''
truncate_cast_movie_rel_staging = '''TRUNCATE cast_movie_rel_staging'''
truncate_reviewer_staging = '''TRUNCATE reviewer_staging'''
truncate_movie_review_staging = '''TRUNCATE movie_review_staging'''


# list to run inserts
insert_table_queries = [movie_info_table_insert, cast_table_insert, cast_movie_rel_table_insert, reviewer_table_insert, movie_review_table]

#list to run staging truncate command
truncate_staging = [truncate_movie_info_staging,truncate_cast_staging,truncate_cast_movie_rel_staging,truncate_reviewer_staging,truncate_movie_review_staging]