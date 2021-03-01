import mysql.connector
from mysql.connector import Error
import csv


# Connects to or creates an instance of the local database and handles database queries
# This database will be used to collect new videos to add to the remote Firebase server database.
# The local database will also act as a way to collect and maintain a remote database backup

def createDataBase():
    try:
        tmpConnection = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="root"
        )
        myCursor = tmpConnection.cursor()
        myCursor.execute("CREATE DATABASE IF NOT EXISTS youtuberandomizer")
    except Error as e:
        print(f"The error '{e}' occurred")


class DatabaseConnector:
    connection = None

    def connectToLocalDb(self):
        try:
            myConnection = mysql.connector.connect(
                host="localhost",
                user="root",
                passwd="root",
                database="youtuberandomizer"
            )
            self.connection = myConnection
            print("Connection to MySQL DB successful")
        except Error as e:
            print(f"The error '{e}' occurred")

    def __init__(self):
        self.connectToLocalDb()
        # If connection isn't successful then we need to create the database
        # (assumes server is running and not the source of the error)
        if self.connection is None:
            print("connection is None, creating database")
            createDataBase()
            self.connectToLocalDb()

    def createVideoDataTable(self):
        query = "CREATE TABLE IF NOT EXISTS videoData(yt_id VARCHAR(15) NOT NULL," \
                " lang VARCHAR(5), upload_date VARCHAR(15), category VARCHAR(3), views INTEGER, " \
                "likes INTEGER, dislikes INTEGER, date_added_db DATE, PRIMARY KEY(yt_id))"
        try:
            myCursor = self.connection.cursor()
            myCursor.execute(query)
        except Error as e:
            print(f" .createScheduleTable: The error '{e}' occurred")

    def insertIntoVideoDataTable(self, dbReader):

        count = 0
        for row in dbReader:
            # Each row has data in the form yt_id, lang, upload_date, category, views, likes, dislikes
            # look for invalid data where the yt_id value = #NAME?
            # look for the case where yt_id leads with a =, remove the =
            if row[0] != "" and row[0] != "#NAME?":
                ytId = row[0]
                if ytId[0] == "=":
                    print(f"removing leading = from {row[0]}")
                    ytId = ytId[1:]
                    print(f"inserting new ytId {ytId}")
                # lang = row[1]
                # upload_date = row[2]
                # category = row[3]
                if row[4] == "":
                    views = -1
                else:
                    views = row[4]

                if row[5] == "":
                    likes = -1
                else:
                    likes = row[5]

                if row[6] == "":
                    dislikes = -1
                else:
                    dislikes = row[6]

                query = "INSERT INTO youtuberandomizer.videodata (yt_id, lang, upload_date, category, views, likes, " \
                        f"dislikes, date_added_db) VALUES('{ytId}', '{row[1]}', '{row[2]}', '{row[3]}', {views}," \
                        f" {likes}, {dislikes}, curdate())"
                try:
                    count += 1
                    myCursor = self.connection.cursor()
                    myCursor.execute(query)
                    self.connection.commit()
                except Error as e:
                    count -= 1
                    # print(f".insertIntoVideoDataTable: The error '{e}' occurred")
                    continue
        return count

    def populateVideoTableFromCSV(self, CSVPath):
        # Try to open the CSV. If successful parse each row of the CSV and insert into the Schedule database
        try:
            with open(f'{CSVPath}') as csvFile:
                reader = csv.reader(csvFile, delimiter=',')
                self.insertIntoVideoDataTable(reader)
        except Error as e:
            print(f"ERROR: populateScheduleFromCSV the error '{e}' occurred")
