#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging
import re

log = logging.getLogger(__name__)

pattern_56bits = re.compile("^(?P<b1>[a-f0-9]{2})(?P<b2>[a-f0-9]{2})"
                              + "(?P<b3>[a-f0-9]{2})(?P<b4>[a-f0-9]{2})"
                              + "(?P<b5>[a-f0-9]{2})(?P<b6>[a-f0-9]{2})"
                              + "(?P<b7>[a-f0-9]{2})$")

pattern_64bits = re.compile("^(?P<b1>[a-f0-9]{2})(?P<b2>[a-f0-9]{2})"
                              + "(?P<b3>[a-f0-9]{2})(?P<b4>[a-f0-9]{2})"
                              + "(?P<b5>[a-f0-9]{2})(?P<b6>[a-f0-9]{2})"
                              + "(?P<b7>[a-f0-9]{2})(?P<b8>[a-f0-9]{2})$")

def obtener_velocidad_haldex(raw):
    match = pattern_64bits.match(raw["speed"])
    if not match:
        return None
    res = int(match.group("b5"), base=16)
    return res

def obtener_frenos_haldex(raw):
    return None

def obtener_hrvd_haldex(raw):
    return None

def obtener_peso_haldex(raw):
    return None

def obtener_ebs_haldex(raw):
    """
    speed:18180000180000
    odometer:00d77c00d21000
    pressures:ff0b52a60a0b6f
    """
    ebs = {}
    ebs["HALDEX"] = raw
    aux = obtener_velocidad_haldex(raw)
    if aux:
        ebs["velocidad"] = aux
    aux = obtener_frenos_haldex(raw)          
    if aux:
        ebs["frenos"] = aux
    aux = obtener_hrvd_haldex(raw)
    if aux:
        ebs["hrvd"] = aux
    aux = obtener_peso_haldex(raw)
    if aux:
        ebs["peso"] = 0    
    return ebs

def obtener_velocidad_knorr(raw):
    match = pattern_64bits.match(raw["speed"])
    if not match:
        return None
    b1 = int(match.group("b1"), base=16)
    b2 = int(match.group("b2"), base=16)
    res = (b2 << 8) | b1
    res = int(res / 256)
    return res  

def obtener_frenos_knorr(raw):
    return None

def obtener_hrvd_knorr(raw):
    match = pattern_64bits.match(raw["hrvd"])
    if not match:
        return None
    b1 = int(match.group("b1"), base=16)
    b2 = int(match.group("b2"), base=16)
    b3 = int(match.group("b3"), base=16)
    b4 = int(match.group("b4"), base=16)
    res = (b4 << 24) | (b3 << 16) | (b2 << 8) | b1
    res = int(res * 0.005)
    return res   

def obtener_peso_knorr(raw):
    match = pattern_64bits.match(raw["weight"])
    if not match:
        return None
    b4 = int(match.group("b4"), base=16)
    b5 = int(match.group("b5"), base=16)
    res = (b5 << 8) | b4
    res = int(res * 2)
    return res  

def obtener_ebs_knorr(raw):
    """
    speed:   b502ffffffffffff
    brakes:  00caffffffffffff
    hrvd:    a8fc6707a8fc6707
    weight:  ffffff4408ffffff
    """
    ebs = {}
    ebs["KNORR"] = raw
    aux = obtener_velocidad_knorr(raw)
    if not aux is None:
        ebs["velocidad"] = aux
    aux = obtener_frenos_knorr(raw)          
    if not aux is None:
        ebs["frenos"] = aux
    aux = obtener_hrvd_knorr(raw)
    if not aux is None:
        ebs["hrvd"] = aux
    aux = obtener_peso_knorr(raw)
    if not aux is None:
        ebs["peso"] = aux
    return ebs

def obtener_ebs_wabco(raw):
    ebs = {}
    ebs["WABCO"] = raw
    return ebs
