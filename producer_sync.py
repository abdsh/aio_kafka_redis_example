from time import sleep
from kafka import KafkaProducer,KafkaConsumer
from json import dumps,loads
import string
import random

alphabet_string = list(string.ascii_lowercase)


producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092',
                         value_serializer=lambda x: dumps(x,default=str).encode('utf-8'),
                         key_serializer=lambda y:str(y).encode('utf-8'))
                         
                         
                         

while True:
        word = random.choice(alphabet_string)
        data = {word:1}
        sleep(1)
        producer.send('source_tw', key=word, value=data)

        print(data)
        print("*****************************************")

    
