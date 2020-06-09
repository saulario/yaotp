#!/usr/bin/python3

import configparser
import datetime
import logging
import optparse
import os
import os.path
import pika
import requests
import sys
import threading
import time
import uuid

from context import Context
from environment import *

log = logging.getLogger(__name__)

class LectorColas(threading.Thread):
    """
    Lee las colas de T-Mobility e inserta directamente en colas AMQP
    """
    def __init__(self, worker):
        super().__init__(name = type(self).__name__)
        self.worker = worker

    def _runImpl(self):
        """
        Lectura de mensajes de T-Mobility y envío por la exchange t-mobility 
        """

        parameters = pika.URLParameters(context.amqp_dbmanager)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        t1 = datetime.datetime.now()
        res = requests.get("%smultipullTarget=%s&multipullMax=%s" 
                        % (context.url, context.queue, context.batch_size), 
                auth=(context.user, context.password))              
        t1 = datetime.datetime.now() - t1

        t2 = datetime.datetime.now()
        mensajes = res.text.splitlines()
        for texto in mensajes:
            props = pika.BasicProperties()
            props.priority = 50
            props.message_id = str(uuid.uuid4())
            props.timestamp = int(time.time())
            channel.basic_publish("tmobility", routing_key = "", 
                    body = bytes(texto, "utf-8"),
                    properties = props, 
                    mandatory = True)
        t2 = datetime.datetime.now() - t2

        connection.close()

    def run(self):
        """
        Ejecución del worker
        """
        while not self.worker.must_stop:
            self._runImpl()
            time.sleep(3)                       # ¿metemos aquí una variable de configuración?


class ProcesarComandos(threading.Thread):
    """
    Escucha la cola de comandos 
    """
    def __init__(self, worker):
        super().__init__(name = type(self).__name__)
        self.worker = worker

    def on_message(self, channel, method_frame, header_frame, body):
        print(method_frame.delivery_tag)
        print(body)
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        if b"STOP" in body:
            log.info("\tRecibido comando STOP...")
            channel.stop_consuming()

    def run(self):
        """
        Escucha la cola de comandos y ejecuta si procede
        """
        log.info("\tEscuchando la cola de comandos...")

        parameters = pika.URLParameters(context.amqp_monitor)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        consumer_tag = ("%s.%s" % (context.instancia, context.proceso))
        channel.basic_consume(queue = "commands",
                on_message_callback=self.on_message,
                consumer_tag=consumer_tag)
        channel.start_consuming()
        connection.close()
        self.worker.must_stop = True


class EnviarEstado(threading.Thread):
    """
    Envía estadísticas de ejecución
    """
    def __init__(self, worker):
        super().__init__(name = type(self).__name__)
        self.worker = worker

    def run(self):
        """
        Envía eventos
        """
        log.info("\tEscuchando la cola de comandos...")

        print("EnviarEstados")

class Worker():
    """
    Clase para coordinar la comunicación entre threads
    """
    def __init__(self, context):
        self.context = context
        self.must_stop = False

    def run(self):
        """
        Lanza los threads
        """
        threads = []

        thread = LectorColas(self)
        threads.append(thread)
        thread.start()

        thread = ProcesarComandos(self)
        threads.append(thread)
        thread.start()

        thread = EnviarEstado(self)
        threads.append(thread)
        thread.start()

        for thread in threads:
            thread.join()


if __name__ == "__main__":

    parser = optparse.OptionParser(usage="usage: %prog [options]")
    parser.add_option("-c", "--config",  default="DESARROLLO.config",
                  help="Fichero de configuración de la instancia (%default)")
    opts, args = parser.parse_args()

    retval = 0
    if opts.config is None:
        sys.stderr.write("Salida: no se ha especificado fichero de configuración\n")
        sys.exit(0)

    try:
        cp = configparser.ConfigParser()
        cp.read("%s/conf/%s" % (DBMANAGER_HOME, opts.config))    
        comprobar_directorios(cp)
    except Exception as e:
        sys.stderr.write(e)
        sys.exit(1)

    context = obtener_contexto_desde_configuracion(cp)
    context.proceso = obtener_nombre_archivo(__file__)
    LOG_FILE = obtener_nombre_archivo_log(context)

    logging.basicConfig(level=logging.INFO, filename=LOG_FILE,
            format=LOG_FORMAT)
    logging.getLogger("pika").setLevel(logging.ERROR)

    log.info("=====> Inicio (%s)" % os.getpid())            

    try:
        if not existe_instancia_activa(context, os.getpid()):
            Worker(context).run()
            borrar_watchdog(context)
        else:
            log.warn("*** Saliendo, existe una instancia en ejecución")
    except Exception as e:
        log.error(e)
        retval = 1

    log.info("<===== Fin (%s)" % os.getpid())
    sys.exit(retval)