#!/usr/bin/python3

import configparser
import datetime
import json
import logging
import mixin
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

class ReaderImpl(threading.Thread,
        mixin.AMQPSendStatsMixin):
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

    SLEEP_TIME = 15

    def __init__(self, worker):
        super().__init__(name = type(self).__name__)
        mixin.AMQPSendStatsMixin.__init__(self, context)

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
        self._stats["estado"] = "running"
        self._ultimo_envio = datetime.datetime.utcnow()


    def _enviar_estadisticas(self, forzar = False):
        """
        Se envían estadísticas pasado un intervalo de n minutos y al terminar
        la ejecución del proceso. Se ejecuta en cada ciclo, pero es aquí 
        donde se controla si ha transcurrido tiempo suficiente
        param: forzar: envío forzado en parada de proceso
        """
        t2 = datetime.datetime.utcnow() - self._ultimo_envio
        if not forzar and t2.seconds < environ.MONITOR_STATS_INTERVAL:
            return
        log.info("\tEnviando estadísticas ...")
        if forzar:
            self._stats["estado"] = "ended"
        self.sendStats(self._stats)
        self._inicializar_estadisticas()


    def _get_mensajes(self):
        """
        Hace petición a T-Mobility y devuelve un array de mensajes. Actualiza
        las estadísticas de peticiones
        """
        t1 = datetime.datetime.now()
        notificaciones = []
        mensajes = []
        try:
            res = requests.get("%smultipullTarget=%s&multipullMax=%s" % (
                        self.context.url, 
                        "Notifications%s" % self.context.queue, 
                        self.context.batch_size), 
                    auth=(self.context.user, self.context.password))              
            notificaciones = res.text.splitlines()
            res = requests.get("%smultipullTarget=%s&multipullMax=%s" % (
                        self.context.url, 
                        self.context.queue, 
                        self.context.batch_size), 
                    auth=(self.context.user, self.context.password))              
            mensajes = res.text.splitlines()
            recibidos = len(notificaciones) + len(mensajes)
            self._stats["peticiones_OK"] += 1
            self._stats["mensajes_recibidos"] += recibidos
        except Exception:
            log.error("\tError conectando a T-Mobility")
            self._stats["peticiones_ERROR"] += 1
            mensajes = ""
        t1 = datetime.datetime.now() - t1   
        log.info("\tRecuperados %d mensajes en %s segundos" % (recibidos, t1))

        return (notificaciones, mensajes)


    def _basic_publish(self, channel, body, properties):
        """
        Reenvía notificaciones y mensajes compartiendo canal
        param: channel: canal de comunicaciones
        param: body: mensaje en texto claro
        param: properties: propiedades del mensaje
        """
        try:
            channel.basic_publish(environ.INSTANCE_TMOBILITY_EXCHANGE, 
                    routing_key = environ.DEFAULT_ROUTING_KEY, 
                    body = bytes(body, "utf-8"),
                    properties = properties, 
                    mandatory = True)
            self._stats["mensajes_enviados"] += 1
        except pika.exceptions.UnroutableError as e:
            log.error("\t*** Error enrutando mensajes de T-Mobility, parando servicio...")
            raise e

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

        props = self.getBasicProperties()
        props.headers[environ.INTERCHANGE_ID_HEADER] = str(uuid.uuid4())

        notificaciones, mensajes = self._get_mensajes()

        for texto in notificaciones:
            props.headers[environ.TYPE_HEADER] = environ.TYPE_HEADER_NOTIFICATION
            self._basic_publish(channel, 
                    body = texto,
                    properties = props)

        for texto in mensajes:
            props.headers[environ.TYPE_HEADER] = environ.TYPE_HEADER_MESSAGE
            self._basic_publish(channel, 
                    body = texto,
                    properties = props)

        connection.close()
        self._enviar_estadisticas()


    def run(self):
        """
        Ejecución del worker
        """
        while not self.worker.must_stop:
            self._runImpl()
            time.sleep(ReaderImpl.SLEEP_TIME)
        self._enviar_estadisticas(forzar = True)

        # mixins
        mixin.AMQPSendStatsMixin.dispose(self)


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
            handlers.BasicWorker(context).run(ReaderImpl)
            environ.borrar_instancia_activa(context)
        else:
            log.warn("*** Saliendo, existe una instancia en ejecución")
    except Exception as e:
        log.error(e)
        retval = 1

    log.info("<===== Fin (%s)" % os.getpid())
    sys.exit(retval)