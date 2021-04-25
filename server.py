import flask
from flask import request, jsonify, make_response
from flask_mysqldb import MySQL
import MySQLdb.cursors
import sqlalchemy
import pandas
import numpy
import json
import Recommenders as Recommenders
from flask_apscheduler import APScheduler
import random

app = flask.Flask(__name__)
app.config["DEBUG"] = True

scheduler = APScheduler()

engine = sqlalchemy.create_engine('mysql://root:test@123@localhost:3306/songs')

app.secret_key = 'my secret key'
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'test@123'
app.config['MYSQL_DB'] = 'songs'

mysql = MySQL(app)
pandas.options.mode.chained_assignment = None

songs = None
ratings = None
merged = None
merged_subset = None
is_model = None

def initializeVariables():
  print('===== Initializing Variables =====')
  songs_tmp = pandas.read_sql_table('music_data', engine)
  ratings_tmp = pandas.read_sql_table('observations', engine)
  merged_tmp = pandas.merge(ratings_tmp, songs_tmp)

  merged_tmp['song'] = merged_tmp['track_name'].map(str) + " - " + merged_tmp['track_artist']
  merged_subset_tmp = merged_tmp.sample(n = 10000)
  # merged_subset_tmp = merged_tmp.head(15000)

  is_model_tmp = Recommenders.item_similarity_recommender_py()
  is_model_tmp.create(merged_subset_tmp, 'user_id', 'song', merged_tmp)

  global songs
  songs = songs_tmp
  global ratings
  ratings = ratings_tmp
  global merged
  merged = merged_tmp
  global merged_subset
  merged_subset = merged_subset_tmp
  global is_model
  is_model = is_model_tmp

  return

initializeVariables()
timeForScheduler = 2*60*60 # 2 hours
scheduler.add_job(id = 'Scheduled Task', func = initializeVariables, trigger = 'interval', seconds = timeForScheduler)
scheduler.start()

@app.route('/genre/<genre_name>', methods=['GET'])
def get_genre_songs(genre_name):
  return songs.loc[songs['playlist_genre'] == genre_name].sort_values(by='track_popularity', ascending=False).head(50).to_json(orient='records')

@app.route('/artist/<artist_name>', methods=['GET'])
def get_artist_songs(artist_name):
  return songs.loc[songs['track_artist'] == artist_name].sort_values(by='track_popularity', ascending=False).head(50).to_json(orient='records')

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
  
  return songs.loc[songs['track_artist'].isin(top_10_artists), ['track_artist', 'artist_image']].drop_duplicates(subset = "track_artist").to_json(orient='records')

@app.route('/genres', methods=['GET'])
def get_genres():
  unique_genres = merged['playlist_genre'].unique().tolist()
  
  return jsonify(unique_genres)

@app.route('/recommend/<user_id>', methods=['GET'])
def get_user_recommendations(user_id):
  # user_items = is_model.get_user_items(user_id)

  # print("------------------------------------------------------------------------------------")
  # print("Training data songs for the user:")
  # print("------------------------------------------------------------------------------------")

  # for user_item in user_items:
  #   print(user_item)

  print("----------------------------------------------------------------------")
  print("Recommendation process going on:")
  print("----------------------------------------------------------------------")

  #Recommend songs for the user using personalized model
  recommendations = is_model.recommend(user_id)

  if isinstance(recommendations, pandas.DataFrame) == False:
    recommendations = pandas.DataFrame()

  # Get all user songs from original observations df
  # Map it to all songs in the subset dataframe
  # This might solve problem where we are not getting any songs for current user.

  return recommendations.to_json(orient='records')

@app.route('/recommend/song/<track_name>', methods=['GET'])
def get_song_recommendations(track_name):
  # track_name = "Ready for Love - 2015 Remaster - Bad Company"
  # track_name = "Soldier - James TW"
  # track_name = "I Feel Alive - Steady Rollin"
  recommendations = is_model.get_similar_items([track_name])

  return recommendations.to_json(orient='records')

@app.route('/', methods=['GET'])
def home():
  return 'Song Recommendation API'

app.run()