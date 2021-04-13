import flask
from flask import request, jsonify, make_response

from flask_mysqldb import MySQL
import MySQLdb.cursors

import sqlalchemy
import pandas
import numpy
import json

app = flask.Flask(__name__)
app.config["DEBUG"] = True

engine = sqlalchemy.create_engine('mysql://root:test@123@localhost:3306/songs')

app.secret_key = 'my secret key'

app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'test@123'
app.config['MYSQL_DB'] = 'songs'

mysql = MySQL(app)

songs = pandas.read_sql_table('music_data', engine)
ratings = pandas.read_sql_table('observations', engine)
merged = pandas.merge(ratings, songs)

@app.route('/song/<track_id>', methods=['GET'])
def get_song(track_id):
  cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
  cursor.execute("SELECT * FROM music_data WHERE track_id = %s", (track_id,))
  song = cursor.fetchone()
  
  print('### Song Details: ', song)
  return jsonify(song)

@app.route('/top/songs', methods=['GET'])
def get_top_10_songs():
  unique_tracks = merged['track_id'].unique()
  listen_counts_for_songs = merged.groupby(['track_id']).sum()['listen_count'].to_numpy()
  sorted_tracks = pandas.Series(data=unique_tracks,index=listen_counts_for_songs).sort_index(ascending = False).tolist()
  sorted_count_for_tracks = -numpy.sort(-listen_counts_for_songs)

  top_10_track_ids = sorted_tracks[:10]
  top_10_track_counts = sorted_count_for_tracks[:10]

  return songs.loc[songs['track_id'].isin(top_10_track_ids)].to_json(orient='records')

@app.route('/top/artists', methods=['GET'])
def get_top_10_artists():
  unique_artists = merged['track_artist'].unique()
  listen_counts_for_artists = merged.groupby(['track_artist']).sum()['listen_count'].to_numpy()
  sorted_artists = pandas.Series(data=unique_artists,index=listen_counts_for_artists).sort_index(ascending = False).tolist()
  sorted_count_for_artists = -numpy.sort(-listen_counts_for_artists)

  top_10_artists = sorted_artists[:10]
  top_10_artists_play_counts = sorted_count_for_artists[:10]
  
  return jsonify(top_10_artists)

@app.route('/genres', methods=['GET'])
def get_genres():
  unique_genres = merged['playlist_genre'].unique().tolist()
  
  return jsonify(unique_genres)

@app.route('/', methods=['GET'])
def home():
  return 'Song Recommendation API'

app.run()