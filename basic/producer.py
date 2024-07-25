
#takes script from streamlit,serilizes it and send to kafka.

import streamlit as st
import asyncio
#from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from pydantic import BaseModel
from confluent_kafka import Producer,Consumer,KafkaException, KafkaError



# transcript = '''
# Oded David went on a walk. The dog was annoying. Then the dog met another dog names Chappy The Dog. Chappy family is the
# Choen's. Joe Choen is a nice person.

# '''
class Paragraph(BaseModel):
    text:str


# kafka set up
kafka_config_producer = {
    'bootstrap.servers': 'localhost:9092'
}
producer_paragraph = Producer(kafka_config_producer)
topic = 'paragraph'



def break_into_paragraphs(transcript):
    # Split the transcript into sentences based on periods
    sentences = transcript.split('.')
    
    # Remove any leading/trailing whitespace from each sentence
    sentences = [sentence.strip() for sentence in sentences if sentence.strip()]
    return sentences

#creates a list of pydantic model instences of paragraph
def pydantic_paragraphs(transcript):
    paragraphs = break_into_paragraphs(transcript)
    paragraph_models = [Paragraph(text=paragraph) for paragraph in paragraphs]
    return paragraph_models

def send_to_kafka(producer, topic, paragraphs):
    for paragraph in paragraphs:
        json_paragraph = paragraph.json() # serializtion for each pydantic paragraph model to json for kafka 
        producer.produce(topic, key = None, value=json_paragraph.encode('utf-8')) 
        print(f'Sent: {json_paragraph}')



def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] key: {msg.key()}, value: {msg.value()}')
        #the value is in byte format so the value will have a b and then the json string



producer_paragraph.flush()
def main():
    st.title("Transcript Submission")

    st.header("Submit Your Transcript")
    transcript = st.text_area("Enter the transcript text here:", height=300)

    if st.button("Submit"):
        if transcript:
            st.success("Transcript submitted successfully!")
            st.write("Here is the transcript you submitted:")
            st.write(transcript)
            serialized_paragraphs = pydantic_paragraphs(transcript)
            send_to_kafka(producer_paragraph,topic,serialized_paragraphs)
        else:
            st.error("Please enter a transcript before submitting.")

if __name__ == "__main__":
    main()
