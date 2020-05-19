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

import parser.analizador as analizador

log = logging.getLogger(__name__)

#
#
#

def procesar(context):
    while True:
        procesarImpl(context)
        time.sleep(5)

def procesarImpl(context):

    t1 = datetime.datetime.now()
    res = requests.get("%smultipullTarget=%s&multipullMax=%s" 
                       % (context.url, context.queue, context.batch_size), 
             auth=(context.user, context.password))              
    t1 = datetime.datetime.now() - t1

    t2 = datetime.datetime.now()
    mensajes = res.text.splitlines()
    for texto in mensajes:
        procesar_mensaje(context, texto)
    t2 = datetime.datetime.now() - t2

    log.info("\tProcesados %s mensajes en %s segundos con una espera de red de %s" % (len(mensajes), t2, t1))
    
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