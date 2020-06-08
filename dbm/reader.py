#!/usr/bin/python3

import configparser
import logging
import optparse
import os
import os.path
import pymongo
import sys

from context import Context
from environment import *

log = logging.getLogger(__name__)

def main(cp):
    pass


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

    CURRENT_INSTANCE = obtener_nombre_instancia(cp)
    CURRENT_FILE = obtener_nombre_archivo(__file__)
    LOG_FILE = obtener_nombre_archivo_log(CURRENT_INSTANCE,
            CURRENT_FILE)

    logging.basicConfig(level=logging.INFO, filename=LOG_FILE,
            format=LOG_FORMAT)
    log.info("=====> Inicio (%s)" % os.getpid())            

    try:
        if not existe_instancia_activa(CURRENT_INSTANCE,
                    CURRENT_FILE, os.getpid()):
            main(cp)
            borrar_watchdog(CURRENT_INSTANCE, CURRENT_FILE)
        else:
            log.warn("*** Saliendo, existe una instancia en ejecución")
    except Exception as e:
        log.error(e)
        retval = 1

    log.info("<===== Fin (%s)" % os.getpid())
    sys.exit(retval)