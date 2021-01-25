# -*- coding: utf-8 -*-
"""
Created on Sat Jan 23 14:50:07 2021

@author: innocentius
"""

import pandas as pd
import tweepy
import gc
import os
import sys
from datetime import datetime
from urllib3.exceptions import ProtocolError
import smtplib
import ssl
import sqlite3
class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        if hasattr(status, 'retweeted_status'): # ignore retweets
            #sys.stdout.write("Retweet, Ignore \n")
            return
        if not status.lang == "en": # You can ignore language here, or you can do it before calling the streamer
            sys.stdout.write("Not English, Ignore \n")
            return
        global tweetcount
        global tweetlist
        tweetcount += 1
        tweetdict = {
            "Tweet_ID": str(status.id),
            "Content": status.text,
            "Created_At": str(status.created_at),
            "Username": status.user.screen_name,
            "User_ID": status.user.id_str,
            "User_Description": status.user.description,
            "User_Followers_count": str(status.user.followers_count),
            "Entities": str(status.entities),
            "Source": status.source,
            "Source_URL": status.source_url,
            "Geo": str(status.geo),
            "Coordinates": str(status.coordinates),
            #"Sensitive": status.possibly_sensitive,
            "Lang": status.lang
            
            } #This only consist a part of what you could put into database, refer to Tweepy API doc for more.
        tweetlist.append(tweetdict)
        sys.stdout.write("Getting tweet No. " + str(tweetcount) + "\n")
        if(tweetcount == 5000): # Data is stored into sqlite3 database, each file containing 5,000 tweets, with a timestamp attached to the file name
            tweetcount = 0
            
            if not os.path.exists("stream"):
                os.mkdir("stream")
            df_result = pd.DataFrame(tweetlist)
            timestamp = datetime.now()
            timestamp_str = timestamp.strftime("%Y_%m_%d_%H_%M_%S")
            filename = os.path.join("stream", timestamp_str + "_data.db")
            conn = sqlite3.connect(filename)
            df_result.to_sql('tweets', conn, if_exists='append', index = None)
            tweetlist = []
        
    def on_error(self, status_code):
        if status_code == 420:
            return False
        print("Continue with status_code:" + str(status_code))
        return True


def main():
    consumer_key = ''
    consumer_secret = ''
    bearer_token = ''
    access_token = ''
    access_token_secret = '' #Insert your Twitter API credential here
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    #api.update_status('tweepy + oauth!')
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth = api.auth, listener = myStreamListener)
    keyword = [] #Insert Your Keywords Here
    while True:
         try:
            #print("In true")
            myStream.filter(track = keyword, languages = ['en'])
         except (ProtocolError, AttributeError) as e: #When a sudden disconnection / Incomplete transfer happens, this helps the program to continue instead of going out of gass
             print(e)
             continue
    
if __name__ == "__main__":
     '''Main Exception Catch, for DEBUG USE ONLY'''
     try:
        tweetcount = 0
        tweetlist = []
        main()
        gc.collect()
     except Exception as e: #The following method could send an email if the program exits unexpectedly, so that you could come back to check.
        print("An Exception has occured:")
        print(e)
        smtp_server = "smtp.gmail.com"
        port = 587
        sender_email = ""
        receiver_email = ""
        message = """\
            Subject: Hi There \n
            
            The program of tweet collecting has stopped.\n
            
            This message is sent from Python.
            """
        password = ""
        
        context = ssl.create_default_context()
        try:
            server = smtplib.SMTP(smtp_server, port)
            server.ehlo()
            server.starttls(context = context)
            server.ehlo()
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message)
        except Exception as e:
            print(e)
        finally:
            server.quit()
        