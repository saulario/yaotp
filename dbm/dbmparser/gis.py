#!/usr/bin/python3
# -*- coding: utf-8 -*-

import re

coordenada_pattern = re.compile("^(?P<grados>\d+):(?P<minutos>\d+):(?P<segundos>\d+\.\d+)(?P<zona>[NSEW])$")

def text2float(coordenada):
    
    retval = None
    match = coordenada_pattern.match(coordenada)
    if (not match):
        return retval

    grados = float(match.group("grados"))
    minutos = float(match.group("minutos"))
    segundos = float(match.group("segundos"))
    zona = match.group("zona")
    
    retval = grados + minutos / 60 + segundos / 3600
    if zona in "SW":
        retval *= (-1)

    return retval

def convertir_coordenada_GPS(latitud, longitud):
    
    retval = None
    
    lat = text2float(latitud)
    if lat is None:
        return retval
    
    lon = text2float(longitud)
    if lon is None:
        return retval

    retval = {}
    retval["type"] = "Point"
    retval["coordinates"] = [ lat, lon ]
    
    return retval    


