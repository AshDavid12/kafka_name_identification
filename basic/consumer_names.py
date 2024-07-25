## recieves the names. 
import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from pydantic import BaseModel
from confluent_kafka import Producer,Consumer,KafkaException, KafkaError
import logging
logging.basicConfig(level=logging.INFO)


kafka_config_consumer = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 'lltttttt',
    'auto.offset.reset': 'earliest' 
}



consumer_names = Consumer(kafka_config_consumer)
topic_out = 'names'
consumer_names.subscribe([topic_out])
def consume_names(consumer,timeout=1.0):
    try:
        logging.info("Starting the consumer for names...")
        while True:
            msg = consumer.poll(timeout)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info(f'{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
                else:
                    raise KafkaException(msg.error())
            else:
                print(f'Received message: {msg.value().decode("utf-8")}') ##getting the names
    except KeyboardInterrupt:
        logging.info("Consumer interrupted.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_names(consumer_names)
