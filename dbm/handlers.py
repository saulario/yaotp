#!/ur/bin/python3

import logging
import pika
import threading

log = logging.getLogger(__name__)

class CommandQueueHandler(threading.Thread):
    """
    Handler encargado de escuchar la cola de comandos para todos los procesos.
    Necesita una referencia al worker que controla para poder activarle el 
    bit de parada, con el que le señaliza que debe terminar su ejecución
    """
    COMMAND_QUEUE = "commands"

    def __init__(self, worker):
        """
        Constructor. Da al thread el mismo nombre que la propia clase
        param: worker: clase que depende de su señalización
        """
        super().__init__(name = type(self).__name__)        
        self.worker = worker 
        self.context = worker.context

    def on_message(self, channel, method_frame, header_frame, body):
        """
        Procesa los comandos recibidos por la cola commands. De momento solo
        controla que la palabra STOP esté en payload, pero hay que implementar
        diferentes opciones dentro de este comando
        """
        if b"STOP" in body:
            log.info("\tRecibido comando STOP...")
            channel.stop_consuming()        

    def run(self):
        """
        Escucha la cola de comandos y ejecuta si procede
        """
        log.info("\tEscuchando la cola de comandos...")

        parameters = pika.URLParameters(self.context.amqp_monitor)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        consumer_tag = ("%s.%s" % (self.context.instancia, self.context.proceso))
        channel.basic_consume(queue = CommandQueueHandler.COMMAND_QUEUE,
                on_message_callback = self.on_message,
                auto_ack = True,
                consumer_tag = consumer_tag)
        channel.start_consuming()
        connection.close()
        self.worker.must_stop = True            


class BasicWorker():
    """
    Clase encargada de lanzar procesos que están controlados por comandos de
    parada recibidos por la cola. Recibe dos subclases de threading.Thread
    siendo la primera el controlador y el segundo el proceso controlado.
    """
    def __init__(self, context):
        self.context = context
        self.must_stop = False

    def run(self, handler, process):
        """
        Lanza la ejecución de un proceso dependiente de un command handler
        param: handler: control de comandos
        param: process: proceso a ejecutar
        """
        log.info("-----> Inicio")

        threads = []

        thread = handler(self)
        threads.append(thread)
        thread.start()
        
        thread = process(self)
        threads.append(thread)
        thread.start()

        for thread in threads:
            thread.join()        

        log.info("<----- Fin")

