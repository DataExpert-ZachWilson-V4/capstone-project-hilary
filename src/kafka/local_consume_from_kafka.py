from confluent_kafka import Consumer, KafkaException
import os

from kafka_helpers import read_config


# Used to test consumption locally as first step
def basic_consume_loop(consumer, topics):
    count = 0
    try:
        consumer.subscribe(topics)

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                count += 1
                print(f"So far consumed {count} messages")
                print(
                    f"Consumed event from topic {msg.topic()}: key = {msg.key().decode('utf-8'):12} value = {msg.value().decode('utf-8'):12}"
                )
    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


config = read_config()
consumer = Consumer(config)
topics = [os.environ["KAFKA_FLIGHTS_TOPIC"]]
basic_consume_loop(consumer, topics)
