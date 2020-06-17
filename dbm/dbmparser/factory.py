#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging
import re

from dbmparser.bat import ParserBATGETINFO
from dbmparser.das import ParserGETDAS
from dbmparser.drvinfo import ParserGETDRIVERINFO
from dbmparser.fmsstat import ParserFMSSTATSGET
from dbmparser.p import ParserP

json_pattern = re.compile("^GPRS\\/SOCKET,(?P<id>[0-9]+),(?P<json>\\{.+\\})$")

log = logging.getLogger(__name__)
    
class ParserNULL(object):
    """
    Parser instanciado por defecto para procesar los mensajes que no son identificados
    """
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
    if not id_dev in context.dispositivos:
        log.warn("<----- Dispositivo %d no encontrado, saliendo..." % id_dev)
        return parser
        
    dispositivo = context.dispositivos[id_dev]
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


def parse(context, properties, body):
    """
    Instancia la familia de parser adecuada y decodifica el mensaje
    """    
    if properties.headers["type"] == "notification":
        return ("notification", "tdi")

    texto = str(body, "utf-8")
    mensaje = parser_factory(context, texto).parse(texto)
        
    return (mensaje, "tdi")
