#/usr/bin/python3
"""
    Recupera mensajes con prefetch y aborta el procesamiento sin pérdida en el 
    topic de origen
"""

import os
import uuid

from proton import ConnectionException, Message
from proton.handlers import MessagingHandler
from proton.reactor import Container, DurableSubscription
from proton.utils import BlockingConnection, BlockingSender


URL = "localhost:5672"
ADDRESS = "topic://tests"
USERNAME = "guest"
PASSWORD = "guest"

CONTAINER_ID = "cl1"
SUBSCRIPTION_NAME = "sn1"

class MyMessageHandler(MessagingHandler):
    def __init__(self, server, address, username, password, name, 
            prefetch = 100, auto_accept = False, auto_settle = False):
        super().__init__(prefetch = prefetch, auto_accept = auto_accept,
                auto_settle = auto_settle)
        self.server = server
        self.address = address
        self.username = username
        self.password = password
        self.name = name

    def on_start(self, event):
        conn = event.container.connect(self.server, user = self.username, 
                password = self.password,
                heartbeat = 60)
        event.container.create_receiver(conn, self.address, name = self.name, 
                options = DurableSubscription())
        pass

    def on_message(self, event):
        print(event.message)
        if "5" in event.message.body:
            self.reject(event.delivery) 
        else:
            self.accept(event.delivery)

    def in_connection_error(self, event):
        raise ConnectionException()

if __name__ == "__main__":

    con = BlockingConnection(url = URL, address = ADDRESS, 
            user = USERNAME, password = PASSWORD)
    sender = BlockingSender(con, con.create_sender(ADDRESS))
    #for i in range(5):
    #   sender.send(Message(("esto es el cuerpo del mensaje %s" % i), id = uuid.uuid4()))
    sender.close()
    con.close()

    container = Container(MyMessageHandler(URL, ADDRESS, USERNAME, PASSWORD,
            SUBSCRIPTION_NAME))
    container.container_id = CONTAINER_ID

    try:
        container.run()
    except Exception as e:
        print(e)



    con.close()