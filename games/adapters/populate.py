import os
from typing import List

from games.adapters.datareader.csvdatareader import GameFileCSVReader
from games.domainmodel.model import Genre, Game, Publisher


def create_tables(connection):
    with connection:
        # obtain a cursor from the connection
        cursor = connection.cursor()

        # create tables in the Database

        cursor.execute('''CREATE TABLE IF NOT EXISTS PUBLISHER(
                            ID INTEGER PRIMARY KEY,
                            NAME  TEXT UNIQUE );''')

        cursor.execute('''CREATE TABLE IF NOT EXISTS GAME(
                      ID INTEGER PRIMARY KEY,
                      TITLE TEXT    NOT NULL,
                      PRICE REAL NOT NULL,
                      RELEASE_DATE TEXT,
                      DESCRIPTION TEXT,
                      IMAGE_URL TEXT,
                      WEBSITE_URL TEXT,
                      PUBLISHER_ID INT NOT NULL,
                      FOREIGN KEY(PUBLISHER_ID) REFERENCES PUBLISHER(ID));
                      ''')

        cursor.execute('''CREATE TABLE IF NOT EXISTS GENRE(
            ID INTEGER PRIMARY KEY  AUTOINCREMENT,
            NAME TEXT    NOT NULL);''')

        cursor.execute('''CREATE TABLE IF NOT EXISTS GAME_GENRE_ASSOC(
            ID INTEGER PRIMARY KEY  AUTOINCREMENT,
            GAME_ID INTEGER NOT NULL,
            GENRE_ID INTEGER NOT NULL,
            FOREIGN KEY(GAME_ID) REFERENCES GAME(ID),
            FOREIGN KEY(GENRE_ID) REFERENCES GENRE(ID));
            ''')

        cursor.execute('''CREATE TABLE IF NOT EXISTS USERS(
                    ID INT PRIMARY KEY     NOT NULL,
                    NAME VARCHAR(50)    NOT NULL,
                    PASSWORD VARCHAR(100)    NOT NULL);''')

        cursor.execute('''CREATE TABLE IF NOT EXISTS REVIEW(
                              GAME_ID INT NOT NULL,
                              USER_ID INT NOT NULL,
                              COMMENT TEXT,
                              RATING INT,
                              TIMESTAMP DATE,
                              Constraint PK_Users_Reviews Primary Key (GAME_ID, USER_ID, TIMESTAMP),
                              FOREIGN KEY(GAME_ID) REFERENCES GAME(ID),
                              FOREIGN KEY(USER_ID) REFERENCES USERS(ID));
                              ''')


def populate_tables(connection):
    dir_name = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    games_file_name = os.path.join(dir_name, "adapters\data\games.csv")

    reader = GameFileCSVReader(games_file_name)

    reader.read_csv_file()

    publishers = reader.dataset_of_publishers
    games = reader.dataset_of_games
    genres = reader.dataset_of_genres

    # Add publishers to the repo
    populate_publisher_table(connection, publishers)

    # Add genres to the repo
    populate_genre_table(connection, genres)

    # Add games to the repo
    populate_games_table(connection, games)

    #populate_genre_game_table(connection)


def populate_publisher_table(connection, publishers: List[Publisher]):
    with connection:
        # obtain a cursor from the connection
        cursor = connection.cursor()
        records = [(index, publisher.publisher_name) for index, publisher in enumerate(publishers, start=1)]
        cursor.executemany('INSERT INTO PUBLISHER VALUES(?,?) ON CONFLICT do nothing', records)
        connection.commit()


def populate_genre_table(connection, genres: List[Genre]):
    with connection:
        cursor = connection.cursor()
        records = [(index, genre.genre_name) for index, genre in enumerate(genres, start=1)]
        cursor.executemany('INSERT INTO GENRE VALUES(?,?) ON CONFLICT do nothing', records)
        connection.commit()


def populate_games_table(connection, games: List[Game]):
    with connection:
        cursor = connection.cursor()
        game_rows = []
        for game in games:
            # get the publisher id of this game from the publisher table
            publisher_id = cursor.execute('SELECT P.ID FROM PUBLISHER P WHERE P.NAME = ?',
                                          (game.publisher.publisher_name,)).fetchone()[0]

            # get the list of genres for this game to populate the game_genre_association table.
            for genre in game.genres:
                genre_id = cursor.execute('SELECT G.ID FROM GENRE G WHERE G.NAME = ?',
                                          (genre.genre_name,)).fetchone()[0]
                populate_genre_game_table(connection, game.game_id, genre_id)

            # create a list of game data row values
            game_rows.append((game.game_id, game.title, game.release_date, game.price, game.description,
                              game.image_url, game.website_url, publisher_id))

        # batch insert all rows
        insert_query = (
            "INSERT INTO GAME (ID, TITLE, RELEASE_DATE, PRICE, DESCRIPTION, IMAGE_URL, WEBSITE_URL, PUBLISHER_ID)"
            " VALUES(?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT do nothing")
        cursor.executemany(insert_query, game_rows)
        connection.commit()


def populate_genre_game_table(connection, game_key, genre_key):
    with connection:
        # obtain a cursor from the connection
        cursor = connection.cursor()
        query = 'INSERT INTO GAME_GENRE_ASSOC (game_id, genre_id) VALUES (:game_id, :genre_id) ON CONFLICT DO NOTHING'
        cursor.execute(
                query, {'game_id': game_key, 'genre_id': genre_key})

        # for genre_key in genre_keys:
        #     query = 'INSERT INTO GAME_GENRE_ASSOC (game_id, genre_id) VALUES (:game_id, :genre_id) ON CONFLICT DO NOTHING'
        #     cursor.execute(
        #         query, {'game_id': game_key, 'genre_id': genre_key})