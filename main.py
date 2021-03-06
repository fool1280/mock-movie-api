from dotenv import load_dotenv
from requests import api
from tqdm import tqdm
import os
import requests
import psycopg2

load_dotenv()
apiKey = os.getenv('API_KEY')
database = os.getenv('DATABASE_NAME')
user = os.getenv('DEFAULT_USER')
password = os.getenv('PASSWORD')
genres = dict()
movies = set()


def getGenres():
    response = requests.get(
        f'https://api.themoviedb.org/3/genre/movie/list?api_key={apiKey}&language=en-US')
    response = response.json()['genres']
    conn = psycopg2.connect(
        host="localhost",
        database=database,
        user=user,
        password=password)
    cursor = conn.cursor()

    # Test if connected successfully
    # cursor.execute("select version()")
    # data = cursor.fetchone()
    # print("Connection established to: ", data)

    # Create table genres with primary id and name
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS Genre (id int PRIMARY KEY, name varchar);")

    # Insert data into table
    global genres
    for i in tqdm(range(len(response))):
        genres[response[i]['id']] = response[i]['name']
        cursor.execute(
            "INSERT INTO Genre (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING;", (response[i]['id'], response[i]['name']))

    # Make the changes to the database persistent
    conn.commit()

    # Close communication with the database
    cursor.close()
    conn.close()
    pass


def getMovies():
    conn = psycopg2.connect(
        host="localhost",
        database=database,
        user=user,
        password=password)
    cursor = conn.cursor()

    cursor.execute(
        "CREATE TABLE IF NOT EXISTS Movie (id int PRIMARY KEY, original_language varchar, title varchar, overview varchar, release_date varchar, poster_path varchar, vote_average real, genre_id int REFERENCES Genre(id));")
    global movies
    for id in genres:
        for i in tqdm(range(1, 6)):
            response = requests.get(
                f'https://api.themoviedb.org/3/discover/movie?api_key={apiKey}&with_genres={id}&page={i}')
            response = response.json()['results']
            for index in tqdm(range(len(response))):
                movie = response[index]
                release_date = ""
                if 'release_date' in movie:
                    release_date = movie['release_date']
                if movie['id'] not in movies:
                    movies.add(movie['id'])
                cursor.execute(
                    "INSERT INTO Movie (id, original_language, title, overview, release_date, poster_path, vote_average, genre_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;", (movie['id'], movie['original_language'], movie['title'], movie['overview'], release_date, movie['poster_path'], movie['vote_average'], id))
    conn.commit()
    conn.close()
    cursor.close()
    pass


def getReviews():
    count = 0
    global movies
    conn = psycopg2.connect(
        host="localhost",
        database=database,
        user=user,
        password=password)
    cursor = conn.cursor()

    cursor.execute(
        "CREATE TABLE IF NOT EXISTS Review (id varchar PRIMARY KEY, author varchar, content varchar, movie_id int REFERENCES Movie(id));")
    for id in movies:
        for count in tqdm(range(1, 4)):
            response = requests.get(
                f'https://api.themoviedb.org/3/movie/{id}/reviews?api_key={apiKey}&language=en-US&page={count}')
            response = response.json()['results']
            if len(response) > 0:
                for i in tqdm(range(len(response))):
                    review = response[i]
                    cursor.execute("INSERT INTO Review (id, author, content, movie_id) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;",
                                   (review['id'], review['author'], review['content'], id))
    conn.commit()
    conn.close()
    cursor.close()
    pass


def getActors():
    count = 0
    global movies
    conn = psycopg2.connect(
        host="localhost",
        database=database,
        user=user,
        password=password)
    gender_mapping = {1: "Female", 2: "Male",
                      0: "Not specified", 3: "Non-binary"}
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS Actor (id int PRIMARY KEY, name varchar, gender varchar);")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS Acts_In (actor_id int REFERENCES Actor(id), movie_id int REFERENCES Movie(id), PRIMARY KEY(actor_id, movie_id));")
    for id in movies:
        response = requests.get(
            f'https://api.themoviedb.org/3/movie/{id}/credits?api_key={apiKey}&language=en-US')
        response = response.json()['cast']
        for cast in response:
            name = cast['name']
            actor_id = cast['id']
            gender = gender_mapping[cast['gender']]
            movie_id = id
            cursor.execute("INSERT INTO Actor (id, name, gender) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING;",
                           (actor_id, name, gender))
            cursor.execute("INSERT INTO Acts_In (actor_id, movie_id) VALUES (%s, %s) ON CONFLICT (actor_id, movie_id) DO NOTHING;",
                           (actor_id, movie_id))
    conn.commit()
    conn.close()
    cursor.close()
    pass


getGenres()
getMovies()
getReviews()
getActors()
