import pika
import threading


class CommandListener(threading.Thread):

    def __init__(self, parent, connection = None, group = None, target = None, 
            name = None, *args, **kwargs):
        super().__init__(group, target, name, *args, **kwargs)
        self.parent = parent
        self.connection = connection

    def on_message(self, channel, method, properties, body):
        print(body)
        if b"STOP" in body:
            channel.close()

    def run(self):
        channel = self.connection.channel()
        channel.basic_consume("commands", self.on_message, auto_ack = False, exclusive = True)
        channel.start_consuming()
        self.parent.must_stop = True



class ProcessExecutor(threading.Thread):
    
    def __init__(self, parent, group = None, target = None, name = None, *args, **kwargs):
        super().__init__(group, target, name, *args, **kwargs)
        self.parent = parent




class ProcessHandler():

    def __init__(self):
        self.must_stop = False
        pass

    def run(self, context = None, target = None, **kwargs):

        credentials = pika.PlainCredentials("guest", "guest")
        parameters = pika.ConnectionParameters("localhost", 5672, "MONITOR", credentials = credentials)
        connection =  sc.SelectConnection(parameters)

        threads = []

        thread = CommandListener(parent = self, connection = connection)
        threads.append(thread)
        thread.start()

        thread = ProcessExecutor(self, target = target)
        threads.append(thread)
        thread.start()

        for thread in threads:
            thread.join()

        connection.close()

class Context:
    pass

def mifuncion():
    print("Estoy imprimiendo mi función")

if __name__ == "__main__":
    pass
    ProcessHandler().run(Context(), mifuncion)
    pass