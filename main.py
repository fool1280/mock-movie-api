from dotenv import load_dotenv
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
        "CREATE TABLE IF NOT EXISTS genres (genre_id int PRIMARY KEY, name varchar);")

    # Insert data into table
    global genres
    for i in tqdm(range(len(response))):
        genres[response[i]['id']] = response[i]['name']
        cursor.execute(
            "INSERT INTO genres (genre_id, name) VALUES (%s, %s) ON CONFLICT (genre_id) DO NOTHING;", (response[i]['id'], response[i]['name']))

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
        "CREATE TABLE IF NOT EXISTS movies (id int PRIMARY KEY, original_language varchar, title varchar, overview varchar, release_date varchar, poster_path varchar, vote_average real, genre_id int REFERENCES genres(genre_id));")
    global movies
    for id in genres:
        for i in tqdm(range(1, 2)):
            response = requests.get(
                f'https://api.themoviedb.org/3/discover/movie?api_key={apiKey}&with_genres={id}&page={i}')
            response = response.json()['results']
            for index in tqdm(range(len(response))):
                movie = response[index]
                if movie['id'] not in movies:
                    movies.add(movie['id'])
                cursor.execute(
                    "INSERT INTO movies (id, original_language, title, overview, release_date, poster_path, vote_average, genre_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;", (movie['id'], movie['original_language'], movie['title'], movie['overview'], movie['release_date'], movie['poster_path'], movie['vote_average'], id))
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
        "CREATE TABLE IF NOT EXISTS reviews (review_id varchar PRIMARY KEY, author varchar, content varchar, movie_id int REFERENCES movies(id));")
    for id in movies:
        for count in tqdm(range(1, 4)):
            response = requests.get(
                f'https://api.themoviedb.org/3/movie/{id}/reviews?api_key={apiKey}&language=en-US&page={count}')
            response = response.json()['results']
            if len(response) > 0:
                for i in tqdm(range(len(response))):
                    review = response[i]
                    cursor.execute("INSERT INTO reviews (review_id, author, content, movie_id) VALUES (%s, %s, %s, %s) ON CONFLICT (review_id) DO NOTHING;",
                                   (review['id'], review['author'], review['content'], 559))
    conn.commit()
    conn.close()
    cursor.close()
    pass


getGenres()
getMovies()
getReviews()
