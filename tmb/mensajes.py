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

import analizador

log = logging.getLogger(__name__)

#
#
#
def procesar(context):
    log.info("-----> Inicio")
    
    res = requests.get("%smultipullTarget=%s&multipullMax=100" 
                       % (context.url, context.queue), 
             auth=(context.user, context.password))              
    mensajes = res.text.splitlines()
    
    for texto in mensajes:
        procesar_mensaje(context, texto)
    
    log.info("<----- Fin")
    
#
#
#
def enviar_mq(context, idMensaje):    
    pass
#
#
#    
def procesar_mensaje(context, texto):
    log.info("-----> Inicio")
    log.info("\t(texto): %s " % texto)
        
    mensaje = analizador.parse(context, texto)
    if not mensaje is None:
        insertedId = context.db.mensajes.insert_one(mensaje).inserted_id
        enviar_mq(context, insertedId)
    
    log.info("<----- Fin")