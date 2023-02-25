import database as db
import setup as set

# Driver code
if __name__ == "__main__":
    """
    Please enter the necessary information related to the DB at this place. 
    Please change PW and ROOT based on the configuration of your own system. 
    """
    PW = "abdul8807"  # IMPORTANT! Put your MySQL Terminal password here.
    ROOT = "root"
    DB = "ecommerece_record"  # This is the name of the database we will create in the next step - call it whatever
    # you like.
    LOCALHOST = "localhost"

    # connection = db.create_server_connection(LOCALHOST, ROOT, PW)
    connection =set.connectToServer()
    # creating the schema in the DB
    set.createDB(connection)
    # db.create_and_switch_database(connection, DB, DB)
    db_connection = set.connectToDB()
    set.createTables(db_connection)
    set.insertUserData(db_connection)
    set.insertProductsData(db_connection)
    set.insertOrdersData(db_connection)
    set.maxOrderValue(db_connection)
    set.minOrderValue(db_connection)
    set.orderMoreThanAvg(db_connection)
    set.createLeaderboardTable(db_connection)
    set.insertIntoLeaderBoard(db_connection)
