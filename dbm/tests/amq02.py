from proton.reactor import Container, DurableSubscription
from proton.utils import BlockingConnection, BlockingReceiver


URL = "localhost:5672"
ADDRESS = "tests"
USERNAME = "guest"
PASSWORD = "guest"

CONTAINER_ID = "cl1"
SUBSCRIPTION_NAME = "sn1"

if __name__ == "__main__":
    
    container = Container()
    container.container_id = "cl1"

    credit = 100
    received = 0

    connection = BlockingConnection(url = URL, container = container,
            user = USERNAME, password = PASSWORD,
            heartbeat = 60)
    receiver = connection.create_receiver(ADDRESS, credit = credit, name = "sn1", 
            options = DurableSubscription())

    while received < credit or True:
        received += 1
        message = receiver.receive()
        if "55" in message.body:
            receiver.release(delivered = True)
        else:
            receiver.accept()
        print("%d %s" % (message.delivery_count, message.body))
        pass

    receiver.detach()
    connection.close()