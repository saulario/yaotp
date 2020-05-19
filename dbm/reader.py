#
#    Copyright (C) from 2017 onwards Saul Correas Subias (saul dot correas at gmail dot com)
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

import configparser
import logging
import os
import pymongo
import sys

import sqlalchemy

import parser.dispositivos as dispositivos
import parser.mensajes as mensajes
import parser.notificaciones as notificaciones

from context import Context
from bl import schema

YAOTP_HOME = ("%s/yaotp" % os.path.expanduser("~"))
YAOTP_CONFIG = ("%s/etc/yaotp.config" % YAOTP_HOME)
YAOTP_LOG = ("%s/log/%s.log" %
             (YAOTP_HOME, os.path.basename(__file__).split(".")[0]))
logging.basicConfig(level=logging.INFO, filename=YAOTP_LOG,
                    format="%(asctime)s %(levelname)s %(module)s.%(funcName)s %(message)s")
log = logging.getLogger(__name__)

#
#
#
if __name__ == "__main__":
    """
    Main module
    """
    log.info("=====> Inicio (%s)" % os.getpid())
    retval = 0

    try:
        cp = configparser.ConfigParser()
        cp.read(YAOTP_CONFIG)

        context = Context()
        context.home = YAOTP_HOME
        context.url = ("%s/tdi/AMMForm?" % cp.get("TDI", "url_formatos"))
        context.queue = cp.get("TDI", "cola")
        context.user = cp.get("TDI", "user")
        context.password = cp.get("TDI", "password")
        context.batch_size = cp.get("TDI", "batch_size")

        context.client = pymongo.MongoClient(cp.get("MONGO", "uri"))
        context.db = context.client.get_database(cp.get("MONGO", "db"))
        context.debug = cp.getint("MONGO", "debug")

        context.sql_engine = sqlalchemy.create_engine("mssql+pymssql://sa:mssql!123@localhost/auroravacia",
                pool_pre_ping = True, pool_recycle = 3600)
        context.sql_metadata = sqlalchemy.MetaData(bind = context.sql_engine)

        dispositivos.procesar(context)
        # notificaciones.procesar(context) 
        # mensajes.procesar(context)

    except Exception as e:
        log.error(e)
        retval = 1
    finally:
        context.client.close()
        context.sql_engine.dispose()

    log.info("<===== Fin (%s)" % os.getpid())
    sys.exit(retval)