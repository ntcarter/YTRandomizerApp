import DatabaseHandler

print("Main Begins")
db = DatabaseHandler.DatabaseConnector()
db.createVideoDataTable()

print("beginning insert:")
a = db.populateVideoTableFromCSV("D:/rawDatasets/completeVideoData3.csv")
b = db.populateVideoTableFromCSV("D:/rawDatasets/completevideodata2.csv")
c = db.populateVideoTableFromCSV("D:/rawDatasets/completeVideoData.csv")

print(f"Sheet1 inserted: {c} Times")
print(f"Sheet2 inserted: {b} Times")
print(f"Sheet3 inserted: {a} Times")


# Create a function that takes a channel ID and scrapes video ID's and other relevant data available
print("Main Ends")
