import mysql.connector
import random
import time
import datetime


# Global methods to push interact with the Database
def insert_in_table(db_connection, statement, params):
    cursor = db_connection.cursor(prepared=True)
    cursor.execute(statement, params)
    db_connection.connection.commit()
    cursor.close()


# def listOfDb(db_connection):
#     cursor = db_connection.cursor(prepared=True)
#     cursor.execute("SHOW DATABASES")
#     for x in cursor:
#         print(x)
#     # return cursor

# This method establishes the connection with the MySQL
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(user=user_name, password=user_password,
                                             host=host_name)
        print('Connected established successfully')
    except mysql.connector.Error as err:
        print(err)
    return connection


# This method will create the database and make it an active database
def create_and_switch_database(connection, db_name):
    cursor = connection.cursor(prepared=True)
    query = "CREATE DATABASE " + db_name
    cursor.execute(query)
    connection.commit()
    cursor.close()


# This method will establish the connection with the newly created DB
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(user=user_name, password=user_password,
                                             host=host_name, database=db_name)
    except mysql.connector.Error as err:
        print(err)
    else:
        if connection.is_connected():
            print("Connected to database " + db_name)
    return connection


# Use this function to create the tables in a database
def create_table(connection, table_creation_statement):
    cursor = connection.cursor(prepared=True)
    cursor.execute(table_creation_statement)
    connection.commit()
    cursor.close()


# Perform all single insert statments in the specific table through a single function call
def create_insert_query(connection, query):
    cursor = connection.cursor(prepared=True)
    cursor.execute(query)
    val = []
    for x in cursor:
        val.append(x)
    return tuple(val)


# retrieving the data from the table based on the given query
def select_query(connection, query, text):
    cursor = connection.cursor(prepared=True)
    cursor.execute(query)
    result = cursor.fetchall()
    print(text)
    for x in result:
        print(x)
    cursor.close()


# Execute multiple insert statements in a table
def insert_many_records(connection, sql, val):
    values = val
    cursor = connection.cursor(prepared=True)
    cursor.executemany(sql, values)
    connection.commit()
    cursor.close()


def close_db_connection(connection):
    if connection.is_connected():
        connection.close()
        print("DB connection closed")
