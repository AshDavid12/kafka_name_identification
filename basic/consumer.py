# takes the sentences from kafka and extracts names that start with capetilized letters. 
#Then it sends the name using another producer to kafkavto the names topic
#import streamlit as st
import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from pydantic import BaseModel
from confluent_kafka import Producer,Consumer,KafkaException, KafkaError
import logging
logging.basicConfig(level=logging.INFO)


class Paragraph(BaseModel):
    text:str

kafka_config_consumer = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 'myyyyuu',
    'auto.offset.reset': 'earliest' 
}

kafka_config_producer_names = {
    'bootstrap.servers': 'localhost:9092'
}
producer_names = Producer(kafka_config_producer_names)
consumer_paragraph = Consumer(kafka_config_consumer)

topic = 'paragraph'
topic_out = 'names'


consumer_paragraph.subscribe([topic])
def produce_names(names): #sending kafka names 
    for name in names:
        producer_names.produce(topic_out, value=name.encode('utf-8'))
        producer_names.flush()
        print(f'Sent: {name}')

def consume_messages(consumer, timeout=1.0):
    try:
        logging.info("Starting the consumer...")
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
                #logging.info(f'Received message: {msg.value().decode("utf-8")}')
                message_text = msg.value().decode("utf-8")
                words = message_text.split()
                capitalized_words = [word for word in words if word.istitle()]
                if capitalized_words:
                    produce_names(capitalized_words)  ##sending words to producer to send names to kafka
    except KeyboardInterrupt:
        logging.info("Consumer interrupted.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages(consumer_paragraph)
   
