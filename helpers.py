import praw
import sqlite3
from praw.models import MoreComments
import pandas as pd
from datetime import datetime
from praw.models import MoreComments

def create_table(conn, table_name):
    cur = conn.cursor()

    if table_name == 'POSTS':
        sql = """CREATE TABLE POSTS (id TEXT PRIMARY KEY, title TEXT, score INTEGER, subreddit TEXT, num_comments INTEGER, body TEXT, created TIMESTAMP);"""

    if table_name == 'COMMENTS':
        sql = "CREATE TABLE COMMENTS (ID TEXT PRIMARY KEY, COMMENTS TEXT, FOREIGN KEY (ID) REFERENCES POSTS (id));"
         
    try:
        cur.execute(sql)
        print('Table {} created!'.format(table_name))
    except:
        print('Table {} already exists!'.format(table_name))

def create_connection(db_file):
    """
    create a database connection to the SQLite database
    specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    
    conn = None
    
    try:
        conn = sqlite3.connect(db_file)
    
    except Error as e:
        print(e)
        
    return conn

def table_to_df(conn, table_name):
    """
    load table as a dataframe
    """
    cur = conn.cursor()
    sql = "SELECT * FROM {}".format(table_name)

    df = pd.read_sql_query(sql, conn)

    return df

def insert_subreddit(conn, subreddit, count):
    """
    make one-line insertion to the specificied table
    :param conn: connection object
    :param subreddit: a set containing what to be inserted
    """
    
    sql = ''' INSERT OR REPLACE INTO POSTS(id, title, score,
         subreddit, num_comments, body, created)
         VALUES(?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    try:
        cur.execute(sql, subreddit)
        conn.commit()
        print("Post {} inserted or replaced successfully.".format(count))
    
    except:
        pass
    
    #return cur.lastrowid

def insert_comment(conn, comment, count):
    """
    make one-line insertion to the COMMENTS table
    :param conn: connection object
    :param comment: a set containing the comments
    """
    
    sql = ''' INSERT OR REPLACE INTO COMMENTS(ID, COMMENTS) VALUES(?,?) '''
    
    cur = conn.cursor()
    
    try:
        cur.execute(sql, comment)
        conn.commit()
        print("Comments of Post {} inserted or replaced successfully.".format(count))
        
    except:
        pass

def print_tablenames(conn):
    """
    print the names of all tables
    :param conn: connection object
    """
    cur = conn.cursor()

    for table in cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall():
        print(table)
