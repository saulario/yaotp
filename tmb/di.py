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
    return di

def remolque_simple(v):
    di = comun(v)
    return di

def remolque_frigorifico_TH14_PF(v):
    di = comun(v)
    return di

def remolque_frigorifico_TH14_AF(v):
    di = comun(v)
    return di

def remolque_frigorifico(v):
    di = comun(v)
    return di

def remolque_frigorifico_noporton(v):
    di = comun(v)
    return di

def remolque_cierre_seguridad_lecitrailer(v):
    di = comun(v)
    return di

def remolque_cierre_seguridad_schmitz(v):
    di = comun(v)
    return di

def furgon_frigorifico(v):
    di = comun(v)
    return di

def furgon_simple(v):
    di = comun(v)
    return di

def tractora_simple(v):
    di = comun(v)
    return di

def tractora_panico(v):
    di = comun(v)
    return di

def tractora_nomasrobos(v):
    di = comun(v)
    return di

def tractora_arintech(v):
    di = comun(v)
    return di

def tractora_portavehiculos(v):
    di = comun(v)
    return di

def tractora_TH14(v):
    di = comun(v)
    return di

def tractora_TH21(v):
    di = comun(v)
    return di

def grua_TH21(v):
    di = comun(v)
    return di

def remolque_TH21(v):
    di = comun(v)
    return di

def obtener_entradas_digitales(v, esquema):
    log.info("-----> Inicio")
    log.info("\t(v) . . .: %s" % v)
    log.info("\t(esquema): %s" % esquema)
    
    if esquema is None:
        log.info("<----- Salida, no hay esquema")
        return None          
    
    if esquema.startswith("Portatil_Simple"):
        f = portatil_simple
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
    di = f(v)
   
    log.info("<----- Fin")
    return di

if __name__ == "__mainn__":
    di = obtener_entradas_digitales(65535, "Portatil_Simple")
    di = obtener_entradas_digitales(65535, "Remolque_Simple")
    di = obtener_entradas_digitales(65535, "Remolque_Frigorifico_TH14_PF")
    di = obtener_entradas_digitales(65535, "Remolque_Frigorifico_TH14_AF")
    di = obtener_entradas_digitales(65535, "Remolque_Frigorifico")
    di = obtener_entradas_digitales(65535, "Remolque_Frigorifico_Noporton")
    di = obtener_entradas_digitales(65535, 
                                    "Remolque_Cierre_Seguridad_Lecitrailer")
    di = obtener_entradas_digitales(65535, "Remolque_Cierre_Seguridad_Schmitz")
    di = obtener_entradas_digitales(65535, "Furgon_Frigorifico")
    di = obtener_entradas_digitales(65535, "Furgon_Simple")
    di = obtener_entradas_digitales(65535, "Tractora_Simple")
    di = obtener_entradas_digitales(65535, "Tractora_Panico")
    di = obtener_entradas_digitales(65535, "Tractora_Nomasrobos")
    di = obtener_entradas_digitales(65535, "Tractora_ArinTech")
    di = obtener_entradas_digitales(65535, "Tractora_Portavehiculos")
    di = obtener_entradas_digitales(65535, "Tractora_TH14")
    di = obtener_entradas_digitales(65535, "Tractora_TH21")
    di = obtener_entradas_digitales(65535, "Grua_TH21")
    di = obtener_entradas_digitales(65535, "Remolque_TH21")