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

import datetime
import logging
import requests
import time

import analizador

log = logging.getLogger(__name__)

#
#
#

def procesar(context):
    while True:
        procesarImpl(context)
        time.sleep(1)

def procesarImpl(context):
    res = requests.get("%smultipullTarget=%s&multipullMax=%s" 
                       % (context.url, context.queue, context.batch_size), 
             auth=(context.user, context.password))              
    mensajes = res.text.splitlines()

    t1 = datetime.datetime.now()
    for texto in mensajes:
        procesar_mensaje(context, texto)
    t1 = datetime.datetime.now() - t1

    log.info("\tProcesados %s mensajes en %s segundos" % (len(mensajes), t1))
    
#
#
#
def enviar_mq(context, idMensaje):    
    pass
#
#
#    
def procesar_mensaje(context, texto):
    try:
        mensaje = analizador.parse(context, texto)
        if not mensaje is None:
            insertedId = context.db.mensajes.insert_one(mensaje).inserted_id
            enviar_mq(context, insertedId)
    except Exception as e:
        log.error(e)
        log.error("*** ignorando mensaje %s" % texto)