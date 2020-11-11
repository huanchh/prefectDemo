import datetime
import json
import sqlite3
from contextlib import closing
from kafka import KafkaConsumer
from prefect import task, Flow
from minio import Minio
from prefect.schedules import IntervalSchedule
import io
from collections import namedtuple

FileTuple = namedtuple('FileTuple', ['fileName', 'meta'])

# signature has to be like so
def alert_failed(obj, old_state, new_state):
    if new_state.is_failed():
        print("failed")

## process image, return image meta data
@task
def process_image(fullFileName, state_handlers=[alert_failed]):
    print("getting meta data for full file name{0}".format(fullFileName))

    minioClient = Minio('127.0.0.1:9000',
                      access_key='minioadmin',
                      secret_key='minioadmin',
                      secure=False)

    splitted = fullFileName.split("/")

    stat = minioClient.stat_object(splitted[0], splitted[1])

    return FileTuple(splitted[1], str(stat))





## put meta data back in bucket
@task
def put_image_data(fileTuple: FileTuple, state_handlers=[alert_failed]):
    fileName, meta = fileTuple
    print("uploading metadata for file name{0}".format(fileName))

    f = io.BytesIO(str.encode(fileTuple.meta))

    minioClient = Minio('127.0.0.1:9000',
                      access_key='minioadmin',
                      secret_key='minioadmin',
                      secure=False)

    minioClient.put_object('images', '{0}.metadata'.format(fileName), f, f.getbuffer().nbytes, 'text/plain')

    print("metadata uploaded for file name{0}".format(fileName))


#ideally we would want to have a more native event driven way to kick off Prefect flow as per
#https://docs.prefect.io/core/PINs/PIN-14-Listener-Flows-2.html
#however this still seem to be in development
def main():
    consumer = KafkaConsumer('bucketevents',
                             group_id='grp1',
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    for msg in consumer:
        # return will cause event to stop
        print("kafka consumer got notified of object event for {0}".format(msg.value.get("Key")))

        # with Flow("my etl flow", schedule) as f:
        with Flow("minio demo flow") as f:
            # with Flow("my etl flow") as f:
            fileName = msg.value.get("Key")
            fileTuple = process_image(fileName)
            put_image_data(fileTuple)

        f.run()



if __name__ == "__main__":
    main()
