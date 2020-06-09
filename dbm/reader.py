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
    Lee las colas de T-Mobility e inserta directamente en colas AMQP. Genera
    estadísticas cada 5 minutos que se envían a través del exchange stats
    del vhost MONITOR.
    Las estadísticas que se envían continen la siguiente información
        1. Id de proceso (instancia.proceso)
        2. Datetime en utc
        3. Peticiones OK
        4. Peticiones ERROR
        5. Mensajes enviados OK
        6. Mensajes enviados ERROR
    """
    def __init__(self, worker):
        super().__init__(name = type(self).__name__)
        self.worker = worker
        self.context = worker.context
        self._inicializar_estadisticas()

    def _inicializar_estadisticas(self):
        """
        Inicializa el objeto con los datos de estadísticas
        """
        self._stats = {}
        self._stats["id_proceso"] = "%s.%s" % (self.context.instancia, 
                self.context.proceso)
        self._stats["timestamp"] = datetime.datetime.utcnow()
        self._stats["peticiones_OK"] = 0
        self._stats["peticiones_ERROR"] = 0
        self._stats["mensajes_recibidos"] = 0
        self._stats["mensajes_enviados"] = 0

    def _get_mensajes(self):
        """
        Hace petición a T-Mobility y devuelve un array de mensajes. Actualiza
        las estadísticas de peticiones
        """

        t1 = datetime.datetime.now()
        mensajes = ""
        try:
            res = requests.get("%smultipullTarget=%s&multipullMax=%s" % (
                        self.context.url, 
                        self.context.queue, 
                        self.context.batch_size), 
                    auth=(self.context.user, self.context.password))              
            mensajes = res.text.splitlines()
            self._stats["peticiones_OK"] += 1
            self._stats["mensajes_recibidos"] += len(mensajes)
        except Exception:
            log.error("\tError conectando a T-Mobility")
            self._stats["peticiones_ERROR"] += 1
            mensajes = ""
        t1 = datetime.datetime.now() - t1   
        log.info("\tRecuperados %d mensajes en %s segundos" % (
                len(mensajes), t1))

        return mensajes

    def _runImpl(self):
        """
        Lectura de mensajes de T-Mobility y envío por la exchange T-Mobility. En
        caso de que no se pueda enrutar un mensaje obtenido de se para inmediatamente
        el servicio.
        Aquí puede haber una pérdida de mensajes, pero eso solo ocurre en el caso de 
        que el exchange no exista, y eso es altamente improbable. Y la conexión y el
        canal ya ha sido abierto antes de hacer la petición a T-Mobility
        """
        parameters = pika.URLParameters(self.context.amqp_dbmanager)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        mensajes = self._get_mensajes()
        for texto in mensajes:
            props = pika.BasicProperties()
            props.priority = 50
            props.message_id = str(uuid.uuid4())
            props.timestamp = int(time.time())
            props.delivery_mode = 2
            try:
                channel.basic_publish("tmobility", routing_key = "", 
                        body = bytes(texto, "utf-8"),
                        properties = props, 
                        mandatory = True)
                self._stats["mensajes_enviados"] += 1
            except pika.exceptions.UnroutableError as e:
                log.error("\t*** Error enrutando mensajes de T-Mobility, parando servicio...")
                raise e

        connection.close()

    def run(self):
        """
        Ejecución del worker
        """
        while not self.worker.must_stop:
            self._runImpl()
            time.sleep(15)                       # ¿metemos aquí una variable de configuración?


class ProcesarComandos(threading.Thread):
    """
    Escucha la cola de comandos 
    """
    def __init__(self, worker):
        super().__init__(name = type(self).__name__)
        self.worker = worker
        self.context = worker.context

    def on_message(self, channel, method_frame, header_frame, body):
        #channel.basic_ack(delivery_tag=method_frame.delivery_tag)
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
        channel.basic_consume(queue = "commands",
                on_message_callback = self.on_message,
                auto_ack = True,
                consumer_tag = consumer_tag)
        channel.start_consuming()
        connection.close()
        self.worker.must_stop = True

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