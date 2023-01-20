import snscrape.modules.twitter as sntwitter
import pandas as pd
import pymongo
import streamlit as st
import datetime
import json

from pymongo import MongoClient
# To Display the title for the project
st.title("Twitter Scrapping Project")
#st.write("Enter the Hashtag")
Hashtag = st.text_input("Enter the HashTag")
# To Select the Start Date and stop Date
# To display the Start date and stop date in same line
col1, col2 = st.columns(2)
with col1:
    Start_Date = st.date_input(
    "Start Date",
    datetime.date(2022, 7, 1))
    st.write('Start Date is:', Start_Date)

with col2:
    Stop_Date = st.date_input(
    "Stop Date",
    datetime.date(2022, 11, 30))
    st.write('Stop Date is:', Stop_Date)

# Converting the Date datatype to String
Stop_Date_S = str(Stop_Date)
Start_Date_S = str(Start_Date)

# to Fix the Limit of the No. of Tweets

Twit_Limit = st.number_input('No. Of Tweets', min_value=1, max_value=1000)

# Making a Button to search the Tweet from the given date range
Search = st.button("Search")

# Making a Button to Store the Tweet in to the database

Store_DB = st.button("Upload")

# Creating list to append tweet data to
tweets_list2 = []

if Search or Store_DB:
# Using TwitterSearchScraper to scrape data and append tweets to list
    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(Hashtag+' since:'+Start_Date_S+
                                    ' until:'+Stop_Date_S).get_items()):
        if i > Twit_Limit:
            break
        tweets_list2.append([tweet.date, tweet.id, tweet.content, tweet.user.username,
                             tweet.replyCount, tweet.retweetCount,
                             tweet.lang, tweet.source, tweet.likeCount])

# Creating a dataframe from the tweets list above
    tweets_df2 = pd.DataFrame(tweets_list2,
                          columns=['Datetime', 'Tweet Id', 'Text', 'Username', 'ReplyCount', 'RetweetCount',
                                   'Language', 'Source', 'LikeCount'])

    st.dataframe(tweets_df2)

# Codes to Store the Datas in to the Data Base

client = MongoClient(("mongodb://localhost:27017"))
# Data Base
db = client["Twitter_Data"]

if Store_DB:
    # Collection

    Chennai_Topic_1 = db[Hashtag]

    tweets_df2.reset_index(inplace=True)
    tweets_df2_dict = tweets_df2.to_dict("records")
    Chennai_Topic_1.insert_one({"index": Hashtag + 'data', "data": tweets_df2_dict})



# Making a Button to download the Tweets from the database

    data_from_db = Chennai_Topic_1.find_one({"index":Hashtag+'data'})
    df = pd.DataFrame(data_from_db["data"])
    df.to_csv("Twitter_data.csv")
    df.to_json("Twitter_data.json")

    st.download_button("Download CSV",
                   df.to_csv(),
                   mime = 'text/csv')

    st.download_button("Download Json",
                       df.to_json(),
                       mime='json')








