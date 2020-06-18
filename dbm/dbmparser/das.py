#!/usr/bin/python3
# -*- coding: utf-8 -*-

class ParserGETDAS():
    """
    Decodificador de mensaje GETDAS
    """
    def __init__(self, c, d):
        self._context = c
        self._dispositivo = d
    
    def parse(self, m):
        mensaje = {}
        mensaje["idDispositivo"] = self._dispositivo["ID_MOVIL"]
        mensaje["matricula"] = self._dispositivo["REGISTRATION"]
        mensaje["tipoMensaje"] = "TDI*GETDAS"  
        mensaje["fechaProceso"] = self._context.ahora
        
        mensaje["raw"] = m
        
        return mensaje