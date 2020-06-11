#!/usr/bin/python3

import configparser
import datetime
import json
import logging
import optparse
import os
import os.path
import pika
import platform
import requests
import sys
import threading
import time
import uuid

# paquetes locales
import context
import environment as environ
import handlers

log = logging.getLogger(__name__)

class DecoderImpl(threading.Thread):
    """
    Lee la cola AMQP tmobility para decodificar los mensajes recibidos. Separa
    en implementaciones separadas por tipos de mensaje e incluso para notificaciones.
    Los mensajes que disparan procesos una vez persistidos se reenvían por el
    exchange AMQP messages, donde se redistribuyen por el routing key. Los routing
    keys activos son

        * tdi.data          -> data
        * tdi.notifications -> notifications
        * ...
    
    El envío de estadísticas se hace a través de la funcionalidad heradada del
    mixin
    """

    TMOBILITY_QUEUE = "tmobility"

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
        self._stats["timestamp"] = str(datetime.datetime.utcnow())
        self._stats["peticiones_OK"] = 0
        self._stats["peticiones_ERROR"] = 0
        self._stats["mensajes_recibidos"] = 0
        self._stats["mensajes_enviados"] = 0
        self._ultimo_envio = datetime.datetime.now()

    def _enviar_estadisticas(self, forzar = False):
        """
        Se envían estadísticas pasado un intervalo de n minutos y al terminar
        la ejecución del proceso. Se ejecuta en cada ciclo, pero es aquí 
        donde se controla si ha transcurrido tiempo suficiente
        param: forzar: envío forzado aunque no haya pasado el tiempo
        """

        if True or False:
            return

        t2 = datetime.datetime.now() - self._ultimo_envio
        if not forzar and t2.seconds < environ.MONITOR_STATS_INTERVAL:
            return
        log.info("\tEnviando estadísticas ...")
        parameters = pika.URLParameters(self.context.amqp_monitor)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        try:
            props = pika.BasicProperties()
            props.headers = {
                 "node" : platform.node()
            }
            channel.basic_publish(environ.MONITOR_STATS_EXCHANGE, 
                    routing_key = "", 
                    body = bytes(json.dumps(self._stats), "utf-8"),
                    properties = props,
                    mandatory = True)
        except pika.exceptions.UnroutableError:
            log.error("\t*** Error enviando estadísticas")
        connection.close()
        self._inicializar_estadisticas()


    def on_message(self, channel, method_frame, header_frame, body):
        """
        Procesa todos los mensajes recibidos, ya sean mensajes o notificaciones.
        """
        print(body)
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        if self.worker.must_stop:
            channel.stop_consuming()

    def run(self):
        """
        Punto de entrada del proceso
        """
        log.info("\tIniciando el decodificador de mensajes...")
        parameters = pika.URLParameters(self.context.amqp_dbmanager)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        consumer_tag = ("%s.%s" % (self.context.instancia, self.context.proceso))
        channel.basic_consume(queue = DecoderImpl.TMOBILITY_QUEUE,
                on_message_callback = self.on_message,
                auto_ack = False,
                consumer_tag = consumer_tag)
        channel.start_consuming()
        connection.close()
        self.worker.must_stop = True       
        self._enviar_estadisticas(forzar = True)


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
        cp.read("%s/conf/%s" % (environ.DBMANAGER_HOME, opts.config))    
        environ.comprobar_directorios(cp)
    except Exception as e:
        sys.stderr.write(e)
        sys.exit(1)

    context = environ.obtener_contexto_desde_configuracion(cp)
    context.proceso = environ.obtener_nombre_archivo(__file__)
    LOG_FILE = environ.obtener_nombre_archivo_log(context)

    logging.basicConfig(level=logging.INFO, filename=LOG_FILE,
            format=environ.LOG_FORMAT)
    logging.getLogger("pika").setLevel(logging.ERROR)

    log.info("=====> Inicio (%s)" % os.getpid())            

    try:
        if not environ.existe_instancia_activa(context, os.getpid()):
            handlers.BasicWorker(context).run(DecoderImpl)
            environ.borrar_instancia_activa(context)
        else:
            log.warn("*** Saliendo, existe una instancia en ejecución")
    except Exception as e:
        log.error(e)
        retval = 1

    log.info("<===== Fin (%s)" % os.getpid())
    sys.exit(retval)