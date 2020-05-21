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

from sqlalchemy.exc import IntegrityError

import parser.analizador as analizador

from bl.schema import *

log = logging.getLogger(__name__)

def procesar(context):
    while True:
        procesarImpl(context)
        time.sleep(1)

def procesarImpl(context):

    conn = context.sql_engine.connect()

    t1 = datetime.datetime.now()
    res = requests.get("%smultipullTarget=%s&multipullMax=%s" 
                       % (context.url, context.queue, context.batch_size), 
             auth=(context.user, context.password))              
    t1 = datetime.datetime.now() - t1

    t2 = datetime.datetime.now()
    mensajes = res.text.splitlines()
    for texto in mensajes:
        procesar_mensaje(context, conn, texto)
    t2 = datetime.datetime.now() - t2

    log.info("\tProcesados %s mensajes en %s segundos"  % (len(mensajes), t2))

    conn.close()
    
def enviar_mq(context, idMensaje):    
    pass

def dal_factory(context, mensaje):
    retval = None
    if "TDI*P" == mensaje["tipoMensaje"]:
        retval = PosicionesDAL(context.sql_metadata)
    return retval

def duplicado(context, mensaje):
    pass

def procesar_mensaje(context, conn, texto):
    try:
        mensaje = analizador.parse(context, texto)
        if mensaje is not None:
            # mongo
            insertedId = context.db.mensajes.insert_one(mensaje).inserted_id
            enviar_mq(context, insertedId)
            # sql
            dal = dal_factory(context, mensaje)
            if dal is not None:
                row = dal.get_instance(mensaje)
                try:
                    dal.insert(conn, **row)
                except IntegrityError as e:
                    # log.warn("\tIgnorando %s %s" % (row["idmovil"], row["FechaHoraGPS"]))
                    pass
                except Exception as e:
                    log.error(e)
                    log.error(row)
                
    except Exception as e:
        log.error(e)
        log.error("\t*** ignorando mensaje %s" % texto)