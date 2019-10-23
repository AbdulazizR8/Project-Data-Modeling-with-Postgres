import os
import glob
import psycopg2
import datetime
import pandas as pd
from sql_queries import *

def insert_song_record(cur, df):
    """Select the relevant Song data from DataFrame and insert it into the songs table."""
    song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df.loc[0, song_columns].values.tolist()
    song_data[3] = song_data[3].item() # Convert dtype from numpy.int64 to int
    cur.execute(song_table_insert, song_data)
    
def insert_artist_record(cur, df):
    """Select the relevant User data from DataFrame and insert it into the artists table."""
    artist_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = df.loc[0, artist_columns].values.tolist()
    cur.execute(artist_table_insert, artist_data)

def process_song_file(cur, filepath):
    """Convert song json file to a Dataframe and extract the song data into our tables."""
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert records into song and artist tables
    insert_song_record(cur, df)
    insert_artist_record(cur, df)

def insert_time_records(cur, df):
    """Construct a time data DataFrame and insert its rows into the time table."""
    time_data = (df['ts'], df['ts'].dt.hour, df['ts'].dt.day, df['ts'].dt.week, df['ts'].dt.month, df['ts'].dt.year, df['ts'].dt.weekday_name)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # Convert tuples to a dict so they can be converted to a DataFrame
    time_dict = dict(zip(column_labels, time_data)) 
    time_df = pd.DataFrame(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        
def insert_user_records(cur, df):
    """Select the relevant User data from DataFrame and insert it into the users table."""
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df.loc[:, user_columns]
    # user_df = user_df.drop_duplicates(subset='userId')

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
def insert_songplay_records(cur, df):
    """
    Construct songplay data and insert the records into our songplays table.
    
    Most data comes from the log data, but we try to get song_id and artist_id by SELECTing         them from the songs and artists tables given song title, artist name, and song duration.
    """
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_log_file(cur, filepath):
    """
    Convert log json file to a Dataframe and extract the log data into our tables.
    
    Filters data by 'NextSong' page.
    Transforms the timestamp column to a datetime before extraction.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df.ts = df['ts'].apply(lambda ts: datetime.datetime.fromtimestamp(ts/1000.0))
    
    # insert records into our users, songplays, and time tables
    insert_time_records(cur, df)
    insert_user_records(cur, df)
    insert_songplay_records(cur, df)

def process_data(cur, conn, filepath, func):
    """Iterate through files in filepath and process them using a given function."""
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Execute our ETL pipeline by connecting to our database and processing the data files."""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()