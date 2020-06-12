#!/usr/bin/python3

import logging
import datetime
import pika
import pickle
import requests
import zlib

# paquetes locales
import environment as environ

log = logging.getLogger(__name__)

class BaseMixin():
    """
    Implementa el método vacío dispose
    """
    def dispose(self):
        pass


class AMQPPublishMixin(BaseMixin):
    """
    Mixin para incorporar la funcionalidad de reenvío por colas AMQP una vez
    decodificado e insertado en base de datos. Al estar reenviando todos los mensajes
    sobre la colas de dbmanager se mantiene la conexión abierta con el servidor.

    Al terminar la ejecución del proceso principal debe invocar al metodo
    dispose() para liberar los recursos que se han reservado
    """

    def __init__(self, context):
        parameters = pika.URLParameters(context.amqp_dbmanager)
        self.__connection = pika.BlockingConnection(parameters)
        self.__channel = self.__connection.channel()


    def publish(self, properties, message, routing_key):
        """
        Reenvía el mensaje para ser consumido por el resto de procesos de 
        dbmanager.
        param: properties: propiedades del mensaje
        param: message: mensaje en texto
        param: routing_key: routing key
        """
        self.__channel.basic_publish(environ.INSTANCE_MESSAGES_EXCHANGE, 
                routing_key = routing_key, 
                body = zlib.compress(pickle.dumps(message)),
                properties = properties,
                mandatory = True)


    def dispose(self):
        """
        Cierre ordenado del canal y la conexión con el broker de mensajes
        """
        self.__channel.close()
        self.__connection.close()

    def __del__(self):
        """
        Previene que hubiera podido quedar abierta la conexión si no se ha invocado
        dispose()
        """
        if self.__connection.is_open:
            self.dispose()


class AMQPSendBackMixin(BaseMixin):
    """
    Mixin para incorporar la funcionalidad de devolución a la cola de origen
    si ha dado algún error de proceso. Como los mensajes solo se devuelven
    en caso de error se asume que es una situación excepcional, por eso se
    abre la conexión en cada rechazo.
    """

    """Número máximo de rebotes de mensaje antes de descartarlo definitivamente"""
    MAX_DELIVERY_COUNT = 10
    MAX_PRIORITY = 99

    def __init__(self, context):
        """
        Constructor
        param: context: contexto de ejecución
        """
        self.__context = context

    def sendBack(self, properties, body):
        """
        Devuelve a la cola de tmobility mensajes que han dado un error. Esto se
        realiza hasta que superan un MAX_DELIVERY_COUNT en que son
        permanentemente descartados. Delivery_count es una cabecera del mensaje,
        si el mensaje llega sin cabeceras este método le crea una básica para
        poder controlar el delivery_count
        Se respetan las cabeceras originales salvo
            1. delivery_count se incrementa en una unidad
            2. priority se incrementa a MAX_PRIORITY
        """
        if properties.headers is None:
            properties.headers = {}
        if not environ.DELIVERY_COUNT_HEADER in properties.headers:
            properties.headers[environ.DELIVERY_COUNT_HEADER] = 0
        properties.headers[environ.DELIVERY_COUNT_HEADER] += 1
        if properties.headers[environ.DELIVERY_COUNT_HEADER] > \
                AMQPSendBackMixin.MAX_DELIVERY_COUNT:
            log.warn("\tIngorando... reintentos superados para " + str(body, "utf-8"))
            return

        properties.priority = AMQPSendBackMixin.MAX_PRIORITY

        parameters = pika.URLParameters(self.context.amqp_dbmanager)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.basic_publish(environ.INSTANCE_TMOBILITY_EXCHANGE, 
                routing_key = environ.DEFAULT_ROUTING_KEY,
                properties = properties,
                body = body)

        channel.close()
        connection.close()


class CargadorDeDispositivosMixin(BaseMixin):
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
        self.__context = context
        self.__last_update = None

    def __lista_obsoleta(self):
        """
        La lista queda obsoleta pasados EXPIRATION_TIME o si no se ha actualizado
        """
        if self.__last_update is None:
            return True
        td = datetime.datetime.now() - self.__last_update
        return td.seconds > CargadorDeDispositivosMixin.EXPIRATION_TIME

    def actualizarDispositivos(self):
        """
        Actualiza los dispositivos en el contexto. 
        """
        if not self.__lista_obsoleta():
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
            self.__context.dispositivos = dd
            self.__last_update = datetime.datetime.now()
