#!/usr/bin/python3

import os
import pymongo
import sqlalchemy

from context import Context

DBMANAGER_VERSION = "1.0"
DBMANAGER_HOME = ("%s/tdi/dbmanager-%s" % (os.path.expanduser("~"), DBMANAGER_VERSION))
LOG_FORMAT = "%(asctime)s %(levelname)s %(threadName)s %(module)s.%(funcName)s %(message)s"


def comprobar_directorios(cp):
    """
    Se comprueban que existen todos los directorios requeridos para la instancia
        - log
        - run
    :param cp: archivo de configuración leído
    """
    INSTANCE_BASE = DBMANAGER_HOME + "/" + cp.get("TDI", "cola")
    INSTANCE_LOG = INSTANCE_BASE + "/log"
    INSTANCE_RUN = INSTANCE_BASE + "/run"
    if not os.path.exists(INSTANCE_BASE):
        os.mkdir(INSTANCE_BASE)
        os.mkdir(INSTANCE_LOG)
        os.mkdir(INSTANCE_RUN)
        return
    
    if not os.path.exists(INSTANCE_LOG):
        os.mkdir(INSTANCE_LOG)

    if not os.path.exists(INSTANCE_RUN):
        os.mkdir(INSTANCE_RUN)        


def existe_instancia_activa(context, pid):
    """
    No puede habar una instancia activa de este proceso
    :param context: contexto de ejecución
    :param pid: id de proceso que entra en ejecución
    """
    watchdog = ("%s/%s/run/%s.pid" % (
            DBMANAGER_HOME,
            context.instancia,
            context.proceso
        ))
    existe = os.path.exists(watchdog)    
    if not existe:
        with open(watchdog, "w") as f:
            f.write(str(pid))
    return existe


def borrar_watchdog(context):
    """
    Cuando un proceso termina normalmente debe eliminar
    el watchdog para que permita un rearranque del proceso
    :param context: contexto de ejecución
    """
    watchdog = ("%s/%s/run/%s.pid" % (
            DBMANAGER_HOME,
            context.instancia,
            context.proceso
        ))
    if os.path.exists(watchdog):
        os.remove(watchdog)


def obtener_nombre_archivo(fichero):
    """
    Para evitar escribir siempre esta instrucción. El nombre del archivo se
    usa como el nombre del proceso, haciendo una equivalencia con argv[0].
    El problema de argv[0] en python es que no necesariamente tiene el mismo
    comportamiento en todos los sistemas operativos
    :param fichero: el valor de __file__ del fichero que se quiere obtener
    """
    return os.path.basename(fichero).split(".")[0]


def obtener_nombre_archivo_log(context):
    """
    Para evitar centralizar la generación del nombre del archivo de log
    :param context: contexto de ejecución
    """
    return ("%s/%s/log/%s.log" % (DBMANAGER_HOME,
            context.instancia, context.proceso))

def obtener_contexto_desde_configuracion(cp):
    """
    Genera un objeto Context desde los parámetros de configuración.
    :param cp: parámetros de configuración
    """
    context = Context()

    context.home = DBMANAGER_HOME
    context.version = DBMANAGER_VERSION
    context.instancia = cp.get("TDI", "cola")

    context.url = ("%s/tdi/AMMForm?" % cp.get("TDI", "url_formatos"))
    context.queue = cp.get("TDI", "cola")
    context.user = cp.get("TDI", "user")
    context.password = cp.get("TDI", "password")
    context.batch_size = cp.get("TDI", "batch_size")

    context.client = pymongo.MongoClient(cp.get("MONGO", "uri"))
    context.db = context.client.get_database(cp.get("MONGO", "db"))
    context.debug = cp.getint("MONGO", "debug")

    context.sql_engine = sqlalchemy.create_engine(cp.get("SQL", "uri"),
            pool_pre_ping = True, pool_recycle = int(cp.get("SQL", "recycle")))
    context.sql_metadata = sqlalchemy.MetaData(bind = context.sql_engine)

    context.amqp_dbmanager = cp.get("AMQP", "dbmanager")
    context.amqp_monitor = cp.get("AMQP", "monitor")

    return context


