import praw
import sys
import sqlite3
from praw.models import MoreComments
from datetime import datetime
from praw.models import MoreComments
from helpers import connect, create_table, insert_comment, insert_subreddit
import config

def main(subreddit_name, num):
    # create a database connect
    #conn = create_connection(config.database)

    conn, c = connect(config.database)

    # create an instance
    reddit = praw.Reddit(client_id=config.client_id, client_secret=config.client_secret, 
                        user_agent=config.user_agent, redirect_uri=config.redirect_uri, username=config.username, 
                        password=config.password)
        
    with conn:
        # create the two tables
        create_table(conn, c, 'POSTS')
        create_table(conn, c, 'COMMENTS')
        
        #obtain an instance of this class for subreddit 
        subreddit = reddit.subreddit(subreddit_name)
        
        count = 0

        #Obtain the top N posts
        #for post in subreddit.top(limit=num):
        for post in subreddit.new(limit = num):
            count += 1
            # convert timestamp to datetime, convert the post.subreddit to str
            post_set = (post.id, post.title, post.score, str(post.subreddit), post.num_comments, \
                post.selftext, datetime.fromtimestamp(post.created))
            
            # insert the post to table 'POSTS'
            insert_subreddit(conn, c, post_set, count)
            
            submission = reddit.submission(id = post.id)
            comments = []
            for top_level_comment in submission.comments:
                try:
                    comments.append(top_level_comment.body)
                except KeyError:
                    comments = [top_level_comment.body]
            comment_set = (post.id, str(comments))

            # insert the comment to table 'COMMENTS'
            insert_comment(conn, c, comment_set, count)
        
    conn.close()


if __name__ == '__main__':
    num_post = int(sys.argv[1])
    main('DataScience', num_post)