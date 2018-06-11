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
    """Procesamiento de la descarga de dispositivos asociados a la cola de mensajes. Renueva el contenido
        de la tabla en cada procesamiento para tener la ultima informacion de las mascaras
    """
    log.info("-----> Inicio")
    
    res = requests.get("%sinfoTarget=%s" % (context.url, context.queue), 
             auth=(context.user, context.password))        
    dispositivos = res.json()
    
    dispositivosCol = context.db.get_collection("dispositivos")
    dispositivosCol.delete_many({})    
       
    key = "INFO_DEVICE"
    if key in dispositivos:
        for dispositivo in dispositivos[key]:
            dispositivo["ID_MOVIL"] = int(dispositivo["ID_MOVIL"])
            dispositivo["fabricante"] = "TDI"
            dispositivo["maskBin"] = int(dispositivo["MASK"][::-1], base=2)
            dispositivo["maskextBin"] = int(dispositivo["MASKEXT"][::-1], 
                       base=2)
            
            dispositivosCol.insert_one(dispositivo)
            
    log.info("<----- Fin")