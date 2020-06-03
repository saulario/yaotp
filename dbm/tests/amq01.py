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
ADDRESS = "tests"
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
        self.expected = prefetch
        self.received = 0

    def on_start(self, event):
        conn = event.container.connect(self.server, user = self.username, 
                password = self.password,
                heartbeat = 60)
        receiver = event.container.create_receiver(conn, self.address, name = self.name,
                options = DurableSubscription())
        pass

    def on_message(self, event):
        if self.expected == 0 or self.received < self.expected:
            self.received += 1
            print("(%d) %s - %d" % (self.received, event.message, event.message.delivery_count))
            if event.message.delivery_count > 10:
                self.reject(event.delivery) 
            #elif "995" in event.message.body:
            #    self.release(event.delivery, True) 
            else:
                self.accept(event.delivery)
            if self.received >= self.expected:
                event.receiver.detach()
                event.connection.close()

    def in_connection_error(self, event):
        raise ConnectionException()

if __name__ == "__main__":

    try:
        while True:
            container = Container(MyMessageHandler(URL, ADDRESS, USERNAME, PASSWORD,
                    SUBSCRIPTION_NAME))
            container.container_id = CONTAINER_ID
            container.run()
            pass 
    except Exception as e:
        print(e)

