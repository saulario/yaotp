#/usr/bin/python3
"""
    Recupera mensajes con prefetch y aborta el procesamiento sin pérdida en el 
    topic de origen
"""

import os
import uuid

from proton import ConnectionException, Delivery, Message
from proton.handlers import MessagingHandler
from proton.reactor import Container, DurableSubscription
from proton.utils import BlockingConnection, BlockingSender


URL = "localhost:5672"
ADDRESS = "tests"
USERNAME = "guest"
PASSWORD = "guest"

CONTAINER_ID = "cl1"
SUBSCRIPTION_NAME = "sn1"

class CloseConnection(Exception):
    pass

class MyMessageHandler(MessagingHandler):

    cuenta = 0

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
        self.connection = None
        self.receiver = None
        self.duplicados = {}

    def on_start(self, event):
        self.connection = event.container.connect(self.server, user = self.username, 
                password = self.password,
                heartbeat = 60)
        self.receiver = event.container.create_receiver(self.connection,
                self.address, name = self.name,
                options = DurableSubscription())
        pass

    def on_message(self, event):
        if self.expected == 0 or self.received < self.expected:
            self.received += 1

            if event.message.id not in self.duplicados:
                self.duplicados[event.message.id] = 1
            else:
                self.duplicados[event.message.id] += 1

            if self.duplicados[event.message.id] >= 3:
                self.reject(event.delivery)
                print("*** descartado %s por %d repeticiones" 
                        % (event.message.id, self.duplicados[event.message.id]))
            elif "95" in event.message.body:
                self.release(event.delivery, True)
            else:
                self.accept(event.delivery)
            
            print("(%s) %s - %d" % (0, event.message, event.message.delivery_count))

            if self.received >= self.expected:
                event.receiver.detach()
                #event.connection.close()
                raise CloseConnection

    def on_connection_closing(self, event):
        pass

    def on_connection_error(self, event):
        raise ConnectionException()

if __name__ == "__main__":

    try:
        while True:
            handler = MyMessageHandler(URL, ADDRESS, USERNAME, PASSWORD, SUBSCRIPTION_NAME)
            container = Container(handler)
            container.container_id = CONTAINER_ID
            container.run()
    except CloseConnection:
        pass
    except Exception as e:
        print(e)

