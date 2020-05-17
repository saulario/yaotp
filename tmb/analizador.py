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
import re

from an_bat import ParserBATGETINFO
from an_das import ParserGETDAS
from an_drvinfo import ParserGETDRIVERINFO
from an_fmsstat import ParserFMSSTATSGET
from an_p import ParserP

json_pattern = re.compile("^GPRS\\/SOCKET,(?P<id>[0-9]+),(?P<json>\\{.+\\})$")

log = logging.getLogger(__name__)
    
class ParserNULL(object):
    
    def __init__(self, c):
        self._context = c
    
    def parse(self, m):
        mensaje = {}
        mensaje["tipoMensaje"] = "PENDIENTE"  
        mensaje["fechaProceso"] = self._context.ahora        
        mensaje["raw"] = m        
        return mensaje

        
def parser_factory(context, texto):
    log.debug("-----> Inicio")
    log.debug("\t(texto): %s" % texto)
    
    parser = ParserNULL(context)
    campos = texto.split(",")
    
    if len(campos) < 3:
        log.warn("<----- Mensaje aparentemente incorrecto, saliendo... %s" % (texto))
        return parser
    
    id_dev = int(campos[1])
    if not id_dev in context.get_dispositivos():
        log.warn("<----- Dispositivo %d no encontrado, saliendo..." % id_dev)
        return parser
        
    dispositivo = context.get_dispositivos()[id_dev]
    tipo = campos[2]
    
    if tipo.startswith("TDI*P="):
        parser = ParserP(context, dispositivo)
    elif tipo.startswith("TDI*BATGETINFO"):
        parser = ParserBATGETINFO(context, dispositivo)            
    elif tipo.startswith("TDI*GETDAS"):
        parser = ParserGETDAS(context, dispositivo)        
    elif tipo.startswith("TDI*GETDRIVER"):
        parser = ParserGETDRIVERINFO(context, dispositivo)
    elif tipo.startswith("TDI*FMSSTATSGET"):
        parser = ParserFMSSTATSGET(context, dispositivo)
        
    log.debug("<----- Fin")
    return parser

def parse(context, texto):
    log.debug("-----> Inicio")
    log.debug("\t(texto): %s" % texto)
    
    mensaje = parser_factory(context, texto).parse(texto)
        
    log.debug("<----- Fin")        
    return mensaje

#if __name__ == "__main__":
#
#    from context import Context
#    c = Context()
#    d = {
#            "ID_MOVIL": 21142,
#            "MASK": "11110000001000101001000000000000000000000100111000",
#            "SCHEMATYPE": "Remolque_Frigorifico",
#            "REGISTRATION": "R1781BCW"
#            }
#    d["maskBin"] = int(d["MASK"][::-1], base=2)
#    c._dispositivos = dict()
#    c._dispositivos[21142] = d
#
#    
#    m = "GPRS/SOCKET,21142,TDI*P=21142,22/04/2018,22:08:27,22/04/2018,22:08:27,041:46:14.5972N,001:13:05.8199W,262,0,113,10,639,628,-32768,-32768,-32768,695,0,0,0,39607.0,49,0,00000000000000,000010a100008a,00d39245001000,f,21403,9,11.77,00bc,aee4,CRCd26d"
#    parse(c, m)

#if __name__ == "__main__":
#
#    from context import Context
#    c = Context()
#    d = {
#            "ID_MOVIL": 21587,
#            "MASK": "11110000001000101000000000000000000000000000011001",
#            "SCHEMATYPE": "Remolque_Simple",
#            "REGISTRATION": "R1781BCW"
#            }
#    d["maskBin"] = int(d["MASK"][::-1], base=2)
#    c._dispositivos = dict()
#    c._dispositivos[21587] = d
#
#    
#    m = "GPRS/SOCKET,21587,TDI*P=21587,01/05/2018,02:23:57,01/05/2018,02:23:57,035:25:54.4693N,084:41:00.8002W,250,113,41,9,1599.7,1200020010000010000000100010,0,310410,7,12.58,0001,ab18,CRCa8ea"
#    parse(c, m)