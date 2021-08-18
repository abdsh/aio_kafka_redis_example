from aiokafka import AIOKafkaConsumer,AIOKafkaProducer
import aioredis
import asyncio
from json import loads,dumps
import uvloop

uvloop.install()

value_de = lambda x: loads(x.decode('utf-8'))
value_se = lambda x: dumps(x,default=str).encode('utf-8')
name = 'test_ss'

async def consume():
    consumer = AIOKafkaConsumer('source_tw',
    bootstrap_servers='127.0.0.1:9092',
    value_deserializer=value_de,
    group_id="g1")

    producer = AIOKafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=value_se,
    key_serializer=lambda y:str(y).encode('utf-8')
    )


    redis = aioredis.from_url("redis://localhost")

    await consumer.start()
    await producer.start()

    try:
        async for msg in consumer:
            #print("consumed: ", list(msg.value.keys())[0], list(msg.value.values())[0])
            incr_value = await redis.zincrby(name, list(msg.value.values())[0], list(msg.value.keys())[0])
            dist_data = {list(msg.value.keys())[0]:int(incr_value)}
            print("***********zincrby***********")
            print(list(msg.value.keys())[0],incr_value)
            await producer.send_and_wait("dest_tw", dist_data)



    finally:
        await consumer.stop()
        await producer.stop()


asyncio.run(consume())