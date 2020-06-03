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

if __name__ == "__main__":

    con = BlockingConnection(url = URL, address = ADDRESS, 
            user = USERNAME, password = PASSWORD)
    sender = BlockingSender(con, con.create_sender(ADDRESS))
    for i in range(1000):
       sender.send(Message(("esto es el cuerpo del mensaje %s" % i), id = uuid.uuid4()))
    sender.close()
    con.close()

