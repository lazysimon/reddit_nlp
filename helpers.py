import praw
import sqlite3
from praw.models import MoreComments
import pandas as pd
from datetime import datetime
from praw.models import MoreComments

def create_table(conn, cur, table_name):

    if table_name == 'POSTS':
        sql = """CREATE TABLE POSTS (id TEXT PRIMARY KEY, title TEXT, score INTEGER, subreddit TEXT, num_comments INTEGER, body TEXT, created TIMESTAMP);"""

    if table_name == 'COMMENTS':
        sql = "CREATE TABLE COMMENTS (ID TEXT PRIMARY KEY, COMMENTS TEXT, FOREIGN KEY (ID) REFERENCES POSTS (id));"
         
    try:
        cur.execute(sql)
        print('Table {} created!'.format(table_name))
    except:
        print('Table {} already exists!'.format(table_name))

def table_to_df(conn, table_name):
    """
    load table as a dataframe
    """
    sql = "SELECT * FROM {}".format(table_name)

    df = pd.read_sql_query(sql, conn)

    return df

def insert_subreddit(conn, cur, subreddit, count):
    """
    make one-line insertion to the specificied table
    :param conn: connection object
    :param subreddit: a set containing what to be inserted
    """
    
    sql = ''' INSERT OR REPLACE INTO POSTS(id, title, score,
         subreddit, num_comments, body, created)
         VALUES(?,?,?,?,?,?,?) '''
    try:
        cur.execute(sql, subreddit)
        conn.commit()
        print("Post {} inserted or replaced successfully.".format(count))
    
    except:
        pass
    
    #return cur.lastrowid

def insert_comment(conn, cur, comment, count):
    """
    make one-line insertion to the COMMENTS table
    :param conn: connection object
    :param comment: a set containing the comments
    """
    
    sql = ''' INSERT OR REPLACE INTO COMMENTS(ID, COMMENTS) VALUES(?,?) '''
    
    try:
        cur.execute(sql, comment)
        conn.commit()
        print("Comments of Post {} inserted or replaced successfully.".format(count))
        
    except:
        pass

def print_tablenames(conn, cur):
    """
    print the names of all tables
    :param conn: connection object
    """

    for table in cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall():
        print(table)

def connect(sqlite_file):
    """ Make connection to an SQLite database file """
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    return conn, c

def close(conn):
    """ Commit changes and close connection to the database """
    # conn.commit()
    conn.close()

def total_rows(cursor, table_name, print_out=False):
    """ Returns the total number of rows in the database """
    cursor.execute('SELECT COUNT(*) FROM {}'.format(table_name))
    count = cursor.fetchall()
    if print_out:
        print('\nTotal rows: {}'.format(count[0][0]))
    return count[0][0]


def table_col_info(cursor, table_name, print_out=False):
    """ Returns a list of tuples with column informations:
    (id, name, type, notnull, default_value, primary_key)
    """
    cursor.execute('PRAGMA TABLE_INFO({})'.format(table_name))
    info = cursor.fetchall()

    if print_out:
        print("\nColumn Info:\nID, Name, Type, NotNull, DefaultVal, PrimaryKey")
        for col in info:
            print(col)
    return info


def values_in_col(cursor, table_name, print_out=True):
    """ Returns a dictionary with columns as keys
    and the number of not-null entries as associated values.
    """
    cursor.execute('PRAGMA TABLE_INFO({})'.format(table_name))
    info = cursor.fetchall()
    col_dict = dict()
    for col in info:
        col_dict[col[1]] = 0
    for col in col_dict:
        cursor.execute('SELECT ({0}) FROM {1} '
                  'WHERE {0} IS NOT NULL'.format(col, table_name))
        # In my case this approach resulted in a
        # better performance than using COUNT
        number_rows = len(cursor.fetchall())
        col_dict[col] = number_rows
    if print_out:
        print("\nNumber of entries per column:")
        for i in col_dict.items():
            print('{}: {}'.format(i[0], i[1]))
    return col_dict

