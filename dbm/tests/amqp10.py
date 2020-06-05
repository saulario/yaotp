import pika
import uuid

HOST = "localhost:5672"
PORT = 5672
EXCHANGE = "tests"
USERNAME = "guest"
PASSWORD = "guest"

if __name__ == "__main__":

    credentials = pika.PlainCredentials("guest", "guest")
    parameters = pika.ConnectionParameters("localhost", 5672, credentials = credentials)
    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()
    for i in range(100):
        mensaje = ("Este es el mensaje %d" % i) 
        headers = { "delivery_count" : 0 }
        props = pika.BasicProperties(message_id = str(uuid.uuid4()), expiration = "300000", 
                headers = headers, priority = 50)
        channel.basic_publish(exchange = EXCHANGE, routing_key = "", body = mensaje,
                 properties = props)

    connection.close()
