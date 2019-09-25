import configparser
import re 
import tweepy 
from tweepy import OAuthHandler 
from textblob import TextBlob
# read config file
config = configparser.ConfigParser()
config.read('capstone.cfg')


class TwitterClient(object): 

    def __init__(self): 

        # keys and tokens from the Twitter Dev Console 
        consumer_key = 'vKhgAF18WuZNHCMixtOZYUlji'
        consumer_secret = 'nOJn5TxFzkqz9mBg5o7gXk7p4Oxd7nbeS4SrXX66WqTP9iYCgl'
        access_token = '24676655-SRLWEntuLhJI8ygV7roXIeCW6lp4m9aiPsKILNWP6'
        access_token_secret = 'mztQws7h8nu0XR5gdlCk9B8nhyIi4WvFrXslt5ytPVbZf'
        
        #intalize twitter api
  			# attempt authentication 
        try: 
            # create OAuthHandler object 
            self.auth = OAuthHandler(consumer_key, consumer_secret) 
            # set access token and secret 
            self.auth.set_access_token(access_token, access_token_secret) 
            # create tweepy API object to fetch tweets 
            self.api = tweepy.API(self.auth) 
        except: 
            print("Error: Authentication Failed") 
  
    #clean the tweets of any special characters or links
    def clean_tweet(self, tweet): 
        #clean the tweets of any special characters or links
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split()) 
  
    #classify sentiment of the tweets
    def get_tweet_sentiment(self, tweet): 

        # create TextBlob object of passed tweet text 
        analysis = TextBlob(self.clean_tweet(tweet)) 
        # set sentiment 
        sentiment_polarity = analysis.sentiment.polarity

        return sentiment_polarity
  
    #seach and return tweets
    def get_tweets(self, query, count = 10): 
          
        # empty list to store parsed tweets 
        tweets = [] 
  
        try: 
            # call twitter api to fetch tweets 
            fetched_tweets = self.api.search(q = query, count = count) 
  
            # parsing tweets one by one 
            for tweet in fetched_tweets: 
                # empty dictionary to store required params of a tweet 
                parsed_tweet = {}
                #print(tweet) 
  
                #saving the user_id, screen name, location, retweet c
                parsed_tweet['movie_title'] = query
                parsed_tweet['user_id'] = tweet.user.id 
                parsed_tweet['screen_name'] = tweet.user.name
                parsed_tweet['location'] = tweet.user.location
                parsed_tweet['retweet_count'] = tweet.retweet_count
                parsed_tweet['favorite_count'] = tweet.favorite_count
                parsed_tweet['created_at'] = tweet.created_at
                
                # saving text of tweet 
                parsed_tweet['text'] = tweet.text 
                # saving sentiment of tweet 
                parsed_tweet['sentiment'] = self.get_tweet_sentiment(tweet.text) 
  
                # appending parsed tweet to tweets list 
                if tweet.retweet_count > 0: 
                    # if tweet has retweets, ensure that it is appended only once 
                    if parsed_tweet not in tweets: 
                        tweets.append(parsed_tweet) 
                else: 
                    tweets.append(parsed_tweet) 
              
            return tweets 
        
        except Exception as e: 
            # print error (if any) 
            print("Error : " + str(e)) 