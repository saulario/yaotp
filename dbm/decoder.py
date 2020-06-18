#!/usr/bin/python3
# -*- coding: utf-8 -*-

import configparser
import datetime
import json
import logging
import optparse
import os
import os.path
import pika
import platform
import sys
import threading
import time
import uuid

# paquetes locales
import bl.schema as schema
import dbmparser.factory
import context
import environment as environ
import handlers
import mixin

log = logging.getLogger(__name__)


class DecoderImpl(threading.Thread, 
        mixin.CargadorDeDispositivosMixin,
        mixin.AMQPPublishMixin, 
        mixin.AMQPSendBackMixin,
        mixin.AMQPSendStatsMixin):
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

    """Tiempo de espera entre iteraciones"""
    SLEEP_TIME = 5

    def __init__(self, worker):
        super().__init__(name = type(self).__name__)
        mixin.CargadorDeDispositivosMixin.__init__(self, context)
        mixin.AMQPPublishMixin.__init__(self, context)
        mixin.AMQPSendBackMixin.__init__(self, context)

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
        self._stats["mensajes_recibidos"] = 0
        self._stats["mensajes_enviados"] = 0        
        self._stats["estado"] = "running"
        self._ultimo_envio = datetime.datetime.utcnow()


    def _enviar_estadisticas(self, forzar = False):
        """
        Se envían estadísticas pasado un intervalo de n minutos y al terminar
        la ejecución del proceso. Se ejecuta en cada ciclo, pero es aquí 
        donde se controla si ha transcurrido tiempo suficiente
        param: forzar: envío forzado aunque no haya pasado el tiempo
        """
        t2 = datetime.datetime.utcnow() - self._ultimo_envio
        if not forzar and t2.seconds < environ.MONITOR_STATS_INTERVAL:
            return
        log.info("\tEnviando estadísticas ...")
        if forzar:
            self._stats["estado"] = "ended"
        self.sendStats(self._stats)
        self._inicializar_estadisticas()

    
    def persist(self, properties = None, mensaje = None):
        """
        Persiste el mensaje en base de datos. Esto de momento está como
        lógica local pero será más limpio si sale a un mixin persistor. Para
        hacer pruebas persiste solo mensajes de posiciones
        """
        if mensaje is None:
            return

        if mensaje["tipoMensaje"] != "TDI*P":
            return

        posicionesDAL = schema.PosicionesDAL(self.context.sql_metadata)
        posicion = posicionesDAL.getInstance(mensaje)
        posicionesDAL.insert(self.__sqlcon, **posicion)

        pass
        

    def _on_message(self, channel, method, properties, body):
        """
        Procesamiento del mensaje
        param: channel: canal
        param: method: frame method
        param: properties: propiedades del mensaje
        param: body: mensaje como array de bytes
        """
        self._stats["mensajes_recibidos"] += 1        
        try:
            mensaje, routing_key = dbmparser.factory.parse(self.context,
                    properties, body)
            self.persist(properties, mensaje)
            self.publish(properties, mensaje, routing_key)
            self._stats["mensajes_enviados"] += 1
        except Exception as e:
            log.warn(e)
            self.sendBack(properties, body)
        channel.basic_ack(method.delivery_tag)
        self._enviar_estadisticas()


    def run(self):
        """
        Punto de entrada del thread. Abre en cada ejecución la conexión con
        la base de datos SQL para no saturar de conexiones abiertas. Esta 
        conexión no está compartida con los mixins, es para uso local.
        """
        parameters = pika.URLParameters(self.context.amqp_dbmanager)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        self.__sqlcon = self.context.sql_engine.connect()

        while not self.worker.must_stop:
            self.actualizarDispositivos()
            method, properties, body = channel.basic_get(
                    environ.INSTANCE_TMOBILITY_EXCHANGE, auto_ack = False)
            while method is not None:
                self._on_message(channel, method, properties, body)
                if self.worker.must_stop:
                    break
                method, properties, body = channel.basic_get(
                        environ.INSTANCE_TMOBILITY_EXCHANGE, auto_ack = False)
            time.sleep(DecoderImpl.SLEEP_TIME)

        self.__sqlcon.close()

        channel.close()
        connection.close()

        self._enviar_estadisticas(forzar = True)

        # mixins
        mixin.CargadorDeDispositivosMixin.dispose(self)
        mixin.AMQPPublishMixin.dispose(self)
        mixin.AMQPSendBackMixin.dispose(self)
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
            handlers.BasicWorker(context).run(DecoderImpl)
            environ.borrar_instancia_activa(context)
        else:
            log.warn("*** Saliendo, existe una instancia en ejecución")
    except Exception as e:
        log.error(e)
        retval = 1

    log.info("<===== Fin (%s)" % os.getpid())
    sys.exit(retval)