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
import parser.analizador as analizador
import context
import environment as environ
import handlers

log = logging.getLogger(__name__)

class CargadorDeDispositivosMixin():
    """
    Mixin para incorporar la funcionalidad de recarga de
    dispositivos desde T-Mobility
    """

    """Expira la cache de dispositivos cada 15 minutos"""
    EXPIRATION_TIME = 900   

    def __init__(self, context):
        """
        Constructor, debe ser invocado para inyectar el contexto
        """
        self._context = context
        self._last_update = None

    def _lista_obsoleta(self):
        """
        La lista queda obsoleta pasados EXPIRATION_TIME o si no se ha actualizado
        """
        if self._last_update is None:
            return True
        td = datetime.datetime.now() - self._last_update
        return td.seconds > CargadorDeDispositivosMixin.EXPIRATION_TIME

    def actualizarDispositivos(self):
        """
        Actualiza los dispositivos en el contexto. 
        """
        if not self._lista_obsoleta():
            return

        log.info("Actualizando dispositivos desde T-Mobility...")
        dispositivos = {}
        try:
            res = requests.get("%sinfoTarget=%s" % (self.context.url, self.context.queue), 
                    auth=(self.context.user, self.context.password))        
            dispositivos = res.json()
        except Exception:
            log.error("*** Error leyendo dispositivos de T-Mobility")
       
        key = "INFO_DEVICE"
        if key in dispositivos:
            dd = {}
            for dispositivo in dispositivos[key]:
                dispositivo["ID_MOVIL"] = int(dispositivo["ID_MOVIL"])
                dispositivo["fabricante"] = "TDI"
                dispositivo["maskBin"] = int(dispositivo["MASK"][::-1], base=2)
                dispositivo["maskextBin"] = int(dispositivo["MASKEXT"][::-1], base=2)
                dd[dispositivo["ID_MOVIL"]] = dispositivo
            self._context.dispositivos = dd
            self._last_update = datetime.datetime.now()


class AMQPSendBackMixin():
    """
    Mixin para incorporar la funcionalidad de devolución a la cola de origen
    si ha dado algún error de proceso
    """

    """Número máximo de rebotes de mensaje antes de descartarlo definitivamente"""
    MAX_DELIVERY_COUNT = 10

    def __init__(self, context):
        self.__context = context

    def sendBack(self, properties, body):
        pass

class AMQPPublishMixin():
    """
    Mixin para incorporar la funcionalidad de reenvío por colas AMQP una vez
    decodificado e insertado en base de datos
    """

    def __init__(self, context):
        self.__context = context

    def publish(self, properties, message):
        pass







class DecoderImpl(threading.Thread, CargadorDeDispositivosMixin,
        AMQPPublishMixin, AMQPSendBackMixin):
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

    SLEEP_TIME = 5

    def __init__(self, worker):
        super().__init__(name = type(self).__name__)
        CargadorDeDispositivosMixin.__init__(self, context)
        AMQPPublishMixin.__init__(self, context)
        AMQPSendBackMixin.__init__(self, context)

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
        

    def _on_message(self, method, properties, body):
        """
        Procesamiento del mensaje
        """
        try:
            mensaje = analizador.parse(self.context, str(body, "utf-8"))
            self.publish(properties, mensaje)
        except Exception as e:
            log.warn(e)
            self.sendBack(properties, body)


    def run(self):
        """
        Punto de entrada del thread
        """
        parameters = pika.URLParameters(self.context.amqp_dbmanager)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        while not self.worker.must_stop:
            self.actualizarDispositivos()
            method, properties, body = channel.basic_get(
                    environ.INSTANCE_TMOBILITY_EXCHANGE, auto_ack = True)
            while method is not None:
                self._on_message(method, properties, body)
                if self.worker.must_stop:
                    break
                method, properties, body = channel.basic_get(
                        environ.INSTANCE_TMOBILITY_EXCHANGE, auto_ack = True)
            time.sleep(DecoderImpl.SLEEP_TIME)

        channel.close()
        connection.close()


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