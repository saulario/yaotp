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
import requests

log = logging.getLogger(__name__)

def procesar(context):
    """Procesamiento de las notificaciones de los dispositivos asociados a la cola
    """
    log.info("-----> Inicio")
    res = requests.get("%smultipullTarget=%s&multipullMax=100" 
                       % (context.url, "Notifications"+ context.queue), 
             auth=(context.user, context.password))              
    mensajes = res.text.splitlines()
    
    for texto in mensajes:
        log.info(texto)
    
    log.info("<----- Fin")