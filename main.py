from dotenv import load_dotenv
import os
import requests
import psycopg2

load_dotenv()
apiKey = os.getenv('API_KEY')
genres = dict()
movies = set()


def getGenres():
    response = requests.get(
        f'https://api.themoviedb.org/3/genre/movie/list?api_key={apiKey}&language=en-US')
    response = response.json()['genres']
    conn = psycopg2.connect(
        host="localhost",
        database="genres",
        user="postgres",
        password="12345")
    cursor = conn.cursor()

    # Test if connected successfully
    # cursor.execute("select version()")
    # data = cursor.fetchone()
    # print("Connection established to: ", data)

    # Create table genres with primary id and name
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS genres (genres_id int PRIMARY KEY, name varchar);")

    # Insert data into table
    global genres
    for i in range(len(response)):
        genres[response[i]['id']] = response[i]['name']
        cursor.execute(
            "INSERT INTO genres (genres_id, name) VALUES (%s, %s) ON CONFLICT (genres_id) DO NOTHING;", (response[i]['id'], response[i]['name']))

    # Make the changes to the database persistent
    conn.commit()

    # Close communication with the database
    cursor.close()
    conn.close()
    pass


def getMovies():
    conn = psycopg2.connect(
        host="localhost",
        database="genres",
        user="postgres",
        password="12345")
    cursor = conn.cursor()

    cursor.execute(
        "CREATE TABLE IF NOT EXISTS movies (id int PRIMARY KEY, original_language varchar, title varchar, overview varchar, release_date date, poster_path varchar, vote_average real, genre_id int REFERENCES genres(genres_id));")
    for id in genres:
        for i in range(1, 2):
            response = requests.get(
                f'https://api.themoviedb.org/3/discover/movie?api_key={apiKey}&with_genres={id}&page={i}')
            response = response.json()['results']
            for index in range(len(response)):
                movie = response[index]
                if movie['id'] in movies:
                    movies.add(movie['id'])
                cursor.execute(
                    "INSERT INTO movies (id, original_language, title, overview, release_date, poster_path, vote_average, genre_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;", (movie['id'], movie['original_language'], movie['title'], movie['overview'], movie['release_date'], movie['poster_path'], movie['vote_average'], id))

    conn.commit()
    conn.close()
    cursor.close()
    pass


getGenres()
getMovies()
