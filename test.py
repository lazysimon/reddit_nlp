import praw
import pandas as pd
import datetime as dt

reddit = praw.Reddit(client_id='Z1grqQBW7ei7hA', client_secret='ekx286gd903s742SoxFSc7mF-kg', 
                     user_agent='simon webscraping test', username='simonneedsleep', 
                     password='ZXW1025reddit!')

print(reddit.user.me())