import pika
import uuid

HOST = "localhost:5672"
PORT = 5672
EXCHANGE = "tests"
USERNAME = "guest"
PASSWORD = "guest"

TOPIC = "cl1"
CONSUMER_TAG = "ct1"


def on_message(channel, method, properties, body):

    print(" [x] %r:%r" % (method.delivery_tag, body))

    if properties.headers["delivery_count"] > 10:
        channel.basic_nack(method.delivery_tag, requeue = False)
        return

    if b"5" in body:
        properties.headers["delivery_count"] += 1
        properties.priority = 100
        channel.basic_publish(exchange = EXCHANGE, routing_key = method.routing_key,
                body = body, properties = properties, mandatory = True)

    channel.basic_ack(method.delivery_tag)

if __name__ == "__main__":

    credentials = pika.PlainCredentials("guest", "guest")
    parameters = pika.ConnectionParameters("localhost", 5672, credentials = credentials)
    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()
    channel.basic_consume(TOPIC, on_message, auto_ack = False, exclusive = True, 
            consumer_tag = CONSUMER_TAG, arguments = None)

    try:
        channel.start_consuming()
    except Exception as e:
        print(e)

    connection.close()
