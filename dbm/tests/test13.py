import pika


def on_connection_open():
    pass

def on_connection_open_error():
    pass

def on_connection_closed():
    pass


if __name__ == "__main__":

    amqp_url = 'amqp://guest:guest@localhost:5672/MONITOR'

    connection = pika.SelectConnection(
            parameters = pika.URLParameters(amqp_url),
            on_open_callback = on_connection_open,
            on_open_error_callback = on_connection_open_error,
            on_close_callback= on_connection_closed)