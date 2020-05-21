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

from bl.schema import *

log = logging.getLogger(__name__)

def procesar(context):
    log.info("-----> Inicio")
    
    res = requests.get("%sinfoTarget=%s" % (context.url, context.queue), 
             auth=(context.user, context.password))        
    dispositivos = res.json()
    
    dispositivosCol = context.db.get_collection("dispositivos")
    dispositivosCol.delete_many({})    

    conn = context.sql_engine.connect()
    movilDAL = MovilDAL(context.sql_metadata)

    moviles = {}
    for movil in movilDAL.query(conn, movilDAL.select()):
        moviles[movil["IdMovil"]] = movil
       
    key = "INFO_DEVICE"
    if key in dispositivos:
        for dispositivo in dispositivos[key]:
            dispositivo["ID_MOVIL"] = int(dispositivo["ID_MOVIL"])
            dispositivo["fabricante"] = "TDI"
            dispositivo["maskBin"] = int(dispositivo["MASK"][::-1], base=2)
            dispositivo["maskextBin"] = int(dispositivo["MASKEXT"][::-1], 
                       base=2)

            dispositivosCol.insert_one(dispositivo)
            id = dispositivo["ID_MOVIL"]
            movil = None
            if not id in moviles:
                movil = {}
                movil["IdMovil"] = dispositivo["ID_MOVIL"]
            else:
                movil = moviles[id]
                
            movil["Matricula"] = dispositivo["REGISTRATION"]
            movil["Nombre"] = dispositivo["CLIENT"]
            movil["Mascara"] = dispositivo["MASK"]
            movil["MascaraExtra"] = dispositivo["MASKEXT"]
            movil["Mail"] = dispositivo["EMAIL"]
            movil["IdTfno"] = dispositivo["TELEPHONE"]

            if not id in moviles:
                movilDAL.insert(conn, **movil)
            else:
                # movilDAL.update(conn, id, **movil)
                pass

    conn.close()
            
    log.info("<----- Fin")