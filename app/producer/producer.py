from os import listdir
from os.path import isfile, join
import time
import base64
from kafka import KafkaProducer
from json import dumps
import uuid

topic = "pothole"

#function to return files in a directory
def fileInDirectory(my_dir: str):
    onlyfiles = [f for f in listdir(my_dir) if isfile(join(my_dir, f))]
    return(onlyfiles)

#function comparing two lists
def listComparison(OriginalList: list, NewList: list):
    differencesList = [x for x in NewList if x not in OriginalList] #Note if files get deleted, this will not highlight them
    return(differencesList)

def publish_images():
    # Start up producer
    producer = KafkaProducer(bootstrap_servers=['kafka:9092'],value_serializer=lambda x: x.encode('utf-8'))

    watchDirectory = r'./images'
    pollTime = 5 #in seconds

    message = None
    while(True):
        if 'watching' not in locals(): #Check if this is the first time the function has run
            previousFileList = fileInDirectory(watchDirectory)
            watching = 1

        time.sleep(pollTime)

        newFileList = fileInDirectory(watchDirectory)

        fileDiff = listComparison(previousFileList, newFileList)

        previousFileList = newFileList
        if len(fileDiff) == 0: continue

        for newFile in fileDiff:
            data = None
            with open(watchDirectory + "/" + newFile, "rb") as image_file:
                data = base64.b64encode(image_file.read())
            message = {
                "id" : str(uuid.uuid1()),
                "name" : newFile,
                "content" : str(data)
            }
            json_data = dumps(message)
            print(json_data)
            producer.send(topic, value=json_data)
            producer.flush()
            print('Message published successfully.')

if __name__ == '__main__':
    print("publishing images!")
    publish_images()
