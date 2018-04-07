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

import logging
import os

import dispositivos
import mensajes
import notificaciones

from Context import Context

YAOTP_HOME = ("%s/yaotp" % os.path.expanduser("~"))
YAOTP_LOG = ("%s/log/reader.log" % YAOTP_HOME)
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
    log.info("-----> Inicio")
    
    context = Context("")
    
    context.home = YAOTP_HOME
    context.url = "http://84.124.55.198:19050/tdi/AMMForm?"
    context.queue = "SESETEST"
    context.user = "tdi"
    context.password = "tdi"

    dispositivos.procesar(context)
    notificaciones.procesar(context)
    mensajes.procesar(context)
    
    context.close()

    log.info("<----- Fin")
    
def reader(args):
    log.info("-----> Inicio")
    
    for key in args.keys():
        log.info("\t(%s): %s" % (key, args[key]))
    
    log.info("<----- Fin")    