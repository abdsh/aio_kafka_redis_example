from aiokafka import AIOKafkaConsumer
from json import loads
import asyncio
import uvloop

uvloop.install()

value_de = lambda x: loads(x.decode('utf-8'))

async def consume():
    consumer = AIOKafkaConsumer(
        'dest_tw',
        bootstrap_servers='127.0.0.1:9092',
        value_deserializer=value_de,
        group_id="g2") 

    await consumer.start()
    try:
        async for msg in consumer:
            print("consumed: ", msg.value)
    finally:
        await consumer.stop()

asyncio.run(consume())