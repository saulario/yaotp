import pika

if __name__ == "__main__":
    params = pika.URLParameters("amqp://guest:guest@localhost:5672/MONITOR")
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    connection.close()