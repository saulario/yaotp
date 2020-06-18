#!/usr/bin/python3
# -*- coding: utf-8 -*-

import itertools
import logging

log = logging.getLogger(__name__)

def comun(v):
    di = {}
    di["gps"] = v & 1
    di["alarmaDI0"] = int(v & 128 != 0)
    di["alarmaDI1"] = int(v & 256 != 0)
    di["alarmaDI2"] = int(v & 512 != 0)
    di["alarmaACCEL"] = int(v & 1024 != 0)
    di["alarmaPP"] = int(v & 2048 != 0)
    return di

def portatil_simple(v):
    di = comun(v)
    return di

def remolque_ABS(v):
    di = comun(v)
    di["absConectado"] = int(v & 16 != 0)
    return di

def remolque_porton(v):
    di = comun(v)
    di["portonCerrado"] = int(v & 64 != 0)
    return di

def remolque_simple(v):
    di = comun(v)
    di["absConectado"] = int(v & 16 != 0)
    di["portonCerrado"] = int(v & 64 != 0)
    return di

def remolque_frigorifico_TH14_PF(v):
    di = comun(v)
    di["portonCerrado"] = int(v & 16 != 0)
    di["frioEncendido"] = int(v & 32 != 0)
    return di

def remolque_frigorifico_TH14_AF(v):
    di = comun(v)
    di["absConectado"] = int(v & 16 != 0)
    di["frioEncendido"] = int(v & 32 != 0)
    return di

def remolque_frigorifico(v):
    di = comun(v)
    di["absConectado"] = int(v & 16 != 0)
    di["frioEncendido"] = int(v & 32 != 0)
    di["portonCerrado"] = int(v & 64 != 0)
    return di

def remolque_frigorifico_noporton(v):
    di = comun(v)
    di["absConectado"] = int(v & 16 != 0)
    di["frioEncendido"] = int(v & 32 != 0)
    return di

def remolque_cierre_seguridad_lecitrailer(v):
    di = comun(v)
    di["portonCerrado"] = int(v & 16 != 0)
    di["cilindroCerrado"] = int(v & 32 != 0)
    di["cubrefallebasCerrado"] = int(v & 64 != 0)
    return di

def remolque_cierre_seguridad_schmitz(v):
    di = comun(v)
    di["absConectado"] = int(v & 16 != 0)
    di["cilindroCerrado"] = int(v & 32 != 0)
    di["portonCerrado"] = int(v & 64 != 0)
    return di

def remolque_cierre_seguridad_lapa5(v):
    di = comun(v)
    di["absConectado"] = int(v & 16 != 0)
    di["cilindroCerrado"] = int(v & 32 != 0)
    di["portonCerrado"] = int(v & 64 != 0)
    return di

def furgon_frigorifico(v):
    di = comun(v)
    di["frioEncendido"] = int(v & 16 != 0)
    di["contactoConectado"] = int(v & 32 != 0)
    di["portonCerrado"] = int(v & 64 != 0)
    return di

def furgon_simple(v):
    di = comun(v)
    di["contactoConectado"] = int(v & 32 != 0)
    di["portonCerrado"] = int(v & 64 != 0)
    return di

def tractora_simple(v):
    di = comun(v)
    di["contactoConectado"] = int(v & 32 != 0)
    return di

def tractora_panico(v):
    di = comun(v)
    di["contactoConectado"] = int(v & 32 != 0)
    di["panicoActivado"] = int(v & 64 != 0)
    return di

def tractora_nomasrobos(v):
    di = comun(v)
    alarma_conectada = (v & 16 != 0)
    di["alarmaConectada"] = int(alarma_conectada)
    if alarma_conectada:
        di["alarmaDisparada"] = int(v & 64 != 0)
    else:
        di["taponAbierto"] = int(v & 64 != 0)
    di["contactoConectado"] = int(v & 32 != 0)
    return di

def tractora_arintech(v):
    di = comun(v)
    di["taponAbierto"] = int(v & 16 != 0)
    di["contactoConectado"] = int(v & 32 != 0)
    di["alarmaDisparada"] = int(v & 64 != 0)
    return di

def tractora_portavehiculos(v):
    di = comun(v)
    di["ptoActivo"] = int(v & 16 != 0)
    di["contactoConectado"] = int(v & 32 != 0)
    return di

def tractora_TH14(v):
    di = comun(v)
    return di

def tractora_TH21(v):
    di = comun(v)
    di["contactoConectado"] = int(v & 2 != 0)
    return di

def grua_TH21(v):
    di = comun(v)
    di["motorCamionEncendido"] = int(v & 2 != 0)
    di["motorGruaEncendido"] = int(v & 4 != 0)
    return di

def remolque_TH21(v):
    di = comun(v)
    di["portonCerrado"] = int(v & 4 != 0)
    return di

def obtener_di_th15(v, esquema):
    
    if esquema is None:
        log.debug("<----- Salida, no hay esquema")
        return None                 
    
    if esquema.startswith("Portatil_Simple"):
        f = portatil_simple
    elif esquema.startswith("Remolque_ABS"):
        f = remolque_ABS
    elif esquema.startswith("Remolque_Porton"):
        f = remolque_porton           
    elif esquema.startswith("Remolque_Simple"):
        f = remolque_simple
    elif esquema.startswith("Remolque_Frigorifico_TH14_PF"):
        f = remolque_frigorifico_TH14_PF
    elif esquema.startswith("Remolque_Frigorifico_TH14_AF"):
        f = remolque_frigorifico_TH14_AF     
    elif esquema.startswith("Remolque_Frigorifico"):
        f = remolque_frigorifico     
    elif esquema.startswith("Remolque_Frigorifico_Noporton"):
        f = remolque_frigorifico_noporton
    elif esquema.startswith("Remolque_Cierre_Seguridad_Lecitrailer"):
        f = remolque_cierre_seguridad_lecitrailer    
    elif esquema.startswith("Remolque_Cierre_Seguridad_Schmitz"):
        f = remolque_cierre_seguridad_schmitz
    elif esquema.startswith("Remolque_Cierre_Seguridad_Lapa5"):
        f = remolque_cierre_seguridad_lapa5        
    elif esquema.startswith("Furgon_Frigorifico"):
        f = furgon_frigorifico
    elif esquema.startswith("Furgon_Simple"):
        f = furgon_simple
    elif esquema.startswith("Tractora_Simple"):
        f = tractora_simple
    elif esquema.startswith("Tractora_Panico"):
        f = tractora_panico
    elif esquema.startswith("Tractora_Nomasrobos"):
        f = tractora_nomasrobos        
    elif esquema.startswith("Tractora_ArinTech"):
        f = tractora_arintech 
    elif esquema.startswith("Tractora_Portavehiculos"):
        f = tractora_portavehiculos
    elif esquema.startswith("Tractora_TH14"):
        f = tractora_TH14        
    elif esquema.startswith("Tractora_TH21"):
        f = tractora_TH21            
    elif esquema.startswith("Grua_TH21"):
        f = grua_TH21           
    elif esquema.startswith("Remolque_TH21"):
        f = remolque_TH21           
    else:
        f = comun
        
    return f(int(v))

def obtener_di_th16(v):
    
    if len(v) < 28:
        return None

    i = itertools.count()
    di = {}
    di["DI0"] = int(v[next(i)])
    di["DI1"] = int(v[next(i)])
    di["DI2"] = int(v[next(i)])
    di["DI3"] = int(v[next(i)])
    di["DI4"] = int(v[next(i)])   
    di["alimentacion"] = int(v[next(i)])
    di["acelerometro1"] = int(v[next(i)])
    di["acelerometro2"] = int(v[next(i)])
    di["activAlimExterna"] = int(v[next(i)])
    di["alarmaDI0"] = int(v[next(i)])
    di["alarmaDI1"] = int(v[next(i)])
    di["alarmaDI2"] = int(v[next(i)])
    di["alarmaDI3"] = int(v[next(i)])
    di["alarmaDI4"] = int(v[next(i)])
    di["alarmaJamming"] = int(v[next(i)])
    di["despertadoDI0"] = int(v[next(i)])
    di["despertadoDI1"] = int(v[next(i)])
    di["despertadoDI2"] = int(v[next(i)])
    di["despertadoDI3"] = int(v[next(i)])
    di["despertadoDI4"] = int(v[next(i)])
    di["despertadoAcelerometro"] = int(v[next(i)])
    di["despertadoTension"] = int(v[next(i)])
    di["despertadoAlimExterna"] = int(v[next(i)])
    di["despertadoTimer"] = int(v[next(i)])
    di["despertadoPDOWN_FAIL"] = int(v[next(i)])
    di["reinicioTH16"] = int(v[next(i)])
    di["gps"] = int(v[next(i)])
    di["alarmaPuntoPaso"] = int(v[next(i)])
    
    return di

def obtener_entradas_digitales(v, esquema, th16):
    log.debug("-----> Inicio")
    log.debug("\t(v) . . .: %s" % v)
    log.debug("\t(esquema): %s" % esquema)
    log.debug("\t(th16)  .: %s" % th16)
    
    if th16:
        di = obtener_di_th16(v)
    else:
        di = obtener_di_th15(v, esquema)
   
    log.debug("<----- Fin")
    return di   