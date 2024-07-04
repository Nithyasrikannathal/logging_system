from kafka import KafkaConsumer
import json
import pymongo
from bson import ObjectId
from pymongo.errors import DuplicateKeyError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def consume_logs():
    consumer = KafkaConsumer(
        'logs',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='log-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client['logging_db']
    collection = db['logs_log']


        # log_data = message.value

        # if '_id' in log_data:
        #     log_data['_id'] = ObjectId(log_data['_id'])

        # #collection.insert_one(log_data)
        # try:
        #     collection.insert_one(log_data)
        # except DuplicateKeyError as e:
        #     print(f"Duplicate key error: {e}")
        # print(f"Inserted log data into MongoDB: {log_data}")
        #log_data = json.loads(message.value) 
    logger.info("Starting to consume logs...")
    for message in consumer:
        log_data = message.value 
        logger.info(f"Received message: {log_data}")
        try:
            collection.update_one({'id': log_data['id']}, {'$set': log_data}, upsert=True)
            logger.info(f"Inserted log: {log_data}")
            consumer.commit()
        except Exception as e:
            print(f"Error inserting log: {e}")
            consumer.commit()


if __name__ == '__main__':
    consume_logs()
