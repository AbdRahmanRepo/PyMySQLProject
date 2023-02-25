import csv
import database as db

PW = "abdul8807"  # IMPORTANT! Put your MySQL Terminal password here.
ROOT = "root"
DB = "ecommerce_record"  # This is the name of the database we will create in the next step - call it whatever you like.
LOCALHOST = "localhost"  # considering you have installed MySQL server on your computer

RELATIVE_CONFIG_PATH = '../config/'

USER = 'users'
PRODUCTS = 'products'
ORDER = 'orders'


def connectToServer():
    # Initial Connection to the SQL Server
    connection = db.create_server_connection(LOCALHOST, ROOT, PW)
    return connection


def createDB(connection):
    # Database creation
    db.create_and_switch_database(connection, DB)


def connectToDB():
    # Connection with required database
    db_connection = db.create_db_connection(LOCALHOST, ROOT, PW, DB)
    return db_connection


def createTables(db_connection):
    userTable = '''CREATE TABLE users ( user_id VARCHAR(10) PRIMARY KEY, user_name VARCHAR(45) , user_email VARCHAR(45) , user_password VARCHAR(45),
    user_address VARCHAR(45), isVentor TINYINT(1))'''

    productTable = '''CREATE TABLE products ( product_id VARCHAR(45) PRIMARY KEY, product_name VARCHAR(45) , 
    product_description VARCHAR(100) , product_price DOUBLE,emi_available VARCHAR(10), vendor_id VARCHAR(10), FOREIGN KEY 
    (vendor_id) REFERENCES users(user_id)) '''

    ordersTable = '''CREATE TABLE orders (order_id INT PRIMARY KEY, total_value DOUBLE ,order_quantity INT , reward_point 
    INT,vendor_id VARCHAR(10), customer_id VARCHAR(10), FOREIGN KEY (customer_id) REFERENCES users(user_id),FOREIGN KEY (
    vendor_id) REFERENCES users(user_id)) '''

    tableList = []
    tableList.append(userTable)
    tableList.append(productTable)
    tableList.append(ordersTable)
    # Required Table creation
    for i in tableList:
        db.create_table(db_connection, i)
        print("Table "+i[12:20]+" Created Successfully")


def insertUserData(db_connection):
    # Data being inserted to the Database
    with open(RELATIVE_CONFIG_PATH + USER + '.csv', 'r') as f:
        val = []
        data = csv.reader(f)
        for row in data:
            val.append(tuple(row))
        insert_users = """INSERT INTO ecommerce_record.users (user_id, user_name, user_email, user_password,
            user_address, isVentor) values (%s, %s, %s, %s, %s, %s) """
        db.insert_many_records(db_connection, insert_users, tuple(val))
        print(USER + ' Data has been inserted successfully')
        # val.pop(0)


def insertProductsData(db_connection):
    with open(RELATIVE_CONFIG_PATH + PRODUCTS + '.csv', 'r') as f:
        val = []
        data = csv.reader(f)
        for row in data:
            val.append(tuple(row))
        insert_statement = """INSERT INTO ecommerce_record.products (product_id, product_name, product_price, product_description,
                vendor_id, emi_available) values (%s, %s, %s, %s, %s, %s) """
        db.insert_many_records(db_connection, insert_statement, tuple(val))
        print(PRODUCTS + ' Data has been inserted successfully')


def insertOrdersData(db_connection):
    with open(RELATIVE_CONFIG_PATH + ORDER + '.csv', 'r') as f:
        val = []
        data = csv.reader(f)
        for row in data:
            val.append(tuple(row))
        insert_statement = """INSERT INTO ecommerce_record.orders (order_id, customer_id, vendor_id, total_value, 
                    order_quantity, reward_point) values (%s, %s, %s, %s, %s, %s) """
        db.insert_many_records(db_connection, insert_statement, tuple(val))
        print(ORDER + ' Data has been inserted successfully')
        # val.pop(0)


def maxOrderValue(db_connection):
    insert_statement = """SELECT MAX(total_value) FROM ecommerce_record.orders"""
    text = 'Maximun order values : '
    db.select_query(db_connection, insert_statement, text)


def minOrderValue(db_connection):
    insert_statement = """SELECT MIN(total_value) FROM ecommerce_record.orders"""
    text = 'Minimum order value : '
    db.select_query(db_connection, insert_statement, text)


def orderMoreThanAvg(db_connection):
    insert_statement = """SELECT order_quantity, AVG(total_value),SUM(total_value) FROM ecommerce_record.orders AS 
    ord GROUP BY order_quantity HAVING SUM(total_value) > AVG(total_value) """
    text = 'The orders of total value more than average value of the total values : '
    db.select_query(db_connection, insert_statement, text)


def createLeaderboardTable(db_connection):
    insert_statement = """CREATE TABLE customer_leaderboard (customer_id VARCHAR(10), total_value DOUBLE ,
    customer_name VARCHAR(50) , customer_email VARCHAR(50)) """
    db.create_table(db_connection, insert_statement)
    print('customer_leaderboard Table created Successfully')


def insertIntoLeaderBoard(db_connection):
    insert_statement = """select ord.customer_id, ord.total_value, usr.user_name, usr.user_email from 
    ecommerce_record.orders as ord join ecommerce_record.users as usr on ord.customer_id = usr.user_id"""
    value = db.create_insert_query(db_connection, insert_statement)
    statement = '''INSERT INTO ecommerce_record.customer_leaderboard (customer_id, total_value, customer_name,
        customer_email) values (%s, %s, %s, %s) '''
    db.insert_many_records(db_connection,statement,value)
    print("All required data has been inserted to customer_leaderboard Table successfully")


# connection = connectToServer()
# createDB(connection)
# db_connection = connectToDB()
# insertIntoLeaderBoard(db_connection)
# createTables(db_connection)
# insertUserData(db_connection)
# insertProductsData(db_connection)
# insertOrdersData(db_connection)
# maxOrderValue(db_connection)
# minOrderValue(db_connection)
# orderMoreThanAvg(db_connection)
