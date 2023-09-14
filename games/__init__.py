import sqlite3
from typing import List

from games.adapters.populate import create_tables, populate_tables
from games.domainmodel.model import Game, Publisher, Genre, User, Review

# Connect to the SQLite database
connection = sqlite3.connect('games.db')


def create_and_populate_db():
    with connection:
        cursor = connection.cursor()

        # Check if any tables exist in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        if len(tables) > 0:
            # At least one table exists, so the database is not empty
            print("The Games database already exists.")
        else:
            # No tables exist, so the database is empty
            print("Creating tables in the database")
            create_tables(connection)
            # populate the DB tables from the provided csv files
            populate_tables(connection)


def execute_query(query: str, parameter=None):
    with connection:
        # obtain a cursor from the connection
        cursor = connection.cursor()
        if parameter is None:
            cursor.execute(query)
        else:
            cursor.execute(query, parameter)


def execute_query_get_all(query: str, parameter=None) -> str:
    with connection:
        # obtain a cursor from the connection
        cursor = connection.cursor()
        if parameter is None:
            cursor.execute(query)
        else:
            cursor.execute(query, parameter)
        result = cursor.fetchall()
        return result


def execute_query_get_one(query: str, parameter=None) -> str:
    with connection:
        # obtain a cursor from the connection
        cursor = connection.cursor()
        if parameter is None:
            cursor.execute(query)
        else:
            cursor.execute(query, parameter)
        result = cursor.fetchone()
        return result[0]


def create_app():
    create_and_populate_db()
    run_queries()


def search_games_by_title(title_string: str, case: bool = False) -> List[Game]:
    pass


def search_games_by_genre(genre_name: str) -> List[Game]:
    pass


def search_games_by_publisher(publisher_name: str) -> List[Game]:
    pass


def run_queries():
    # Print the total number of games from the database
    query = '''SELECT  COUNT(*) from GAME'''
    print(f"Total games : {execute_query_get_one(query)}")

    # Print the details of the first 10 games in the db
    query = '''SELECT  * from GAME LIMIT 10'''
    games = execute_query_get_all(query)
    print(f"Printing details of first 10 games")
    print(*games, sep='\n')

    # Write the queries for the following search functions
    # Retrieve games based on title, publisher or genre as specified by the user
    # # These should all be case-insensitive searches
    # without leading and trailing spaces capable of supporting partial search

    # title_string = input("Enter the search string for game titles : ")
    # searched_games = search_games_by_title(title_string, case=True)
    # print(f"Total games with {title_string} in title is {len(searched_games)}")
    # print(*searched_games, sep='\n')

    # publisher_string = input("Enter the search string for publisher name : ")
    # searched_games = search_games_by_publisher(publisher_string)
    # print(f"Total publishers {publisher_string} in name is {len(searched_games)}")
    # print(*searched_games, sep='\n')

    # genre_name = input("Enter genre name : ")
    # searched_games = search_games_by_genre(genre_name)
    # print(f"Total games with genre {genre_name} is {len(searched_games)}")
    # print(*searched_games, sep='\n')
