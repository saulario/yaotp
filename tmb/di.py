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
    return di

def portatil_simple(v):
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
    
    switch = {
            "Portatil_Simple": portatil_simple,
            "Remolque_Simple": remolque_simple,
            "Remolque_Frigorifico_TH14_PF": remolque_frigorifico_TH14_PF,
            "Remolque_Frigorifico_TH14_AF": remolque_frigorifico_TH14_AF,
            "Remolque_Frigorifico": remolque_frigorifico,
            "Remolque_Frigorifico_Noporton": remolque_frigorifico_noporton,
            "Remolque_Cierre_Seguridad_Lecitrailer": 
                remolque_cierre_seguridad_lecitrailer,
            "Remolque_Cierre_Seguridad_Schmitz": 
                remolque_cierre_seguridad_schmitz,
            "Furgon_Frigorifico": furgon_frigorifico,
            "Furgon_Simple": furgon_simple,
            "Tractora_Simple": tractora_simple,     
            "Tractora_Panico": tractora_panico,
            "Tractora_Nomasrobos": tractora_nomasrobos,
            "Tractora_ArinTech": tractora_arintech,
            "Tractora_Portavehiculos": tractora_portavehiculos,
            "Tractora_TH14": tractora_TH14,
            "Tractora_TH21": tractora_TH21,
            "Grua_TH21": grua_TH21,
            "Remolque_TH21": remolque_TH21
            }
    di = switch.get(esquema, comun)(v)
    
    log.info("<----- Fin")
    return di

#if __name__ == "__main__":
#    di = obtener_entradas_digitales("f", "Portatil_Simple")
#    di = obtener_entradas_digitales("f", "Remolque_Simple")
#    di = obtener_entradas_digitales("f", "Remolque_Frigorifico_TH14_PF")
#    di = obtener_entradas_digitales("f", "Remolque_Frigorifico_TH14_AF")
#    di = obtener_entradas_digitales("f", "Remolque_Frigorifico")
#    di = obtener_entradas_digitales("f", "Remolque_Frigorifico_Noporton")
#    di = obtener_entradas_digitales("f", 
#                                    "Remolque_Cierre_Seguridad_Lecitrailer")
#    di = obtener_entradas_digitales("f", "Remolque_Cierre_Seguridad_Schmitz")
#    di = obtener_entradas_digitales("f", "Furgon_Frigorifico")
#    di = obtener_entradas_digitales("f", "Furgon_Simple")
#    di = obtener_entradas_digitales("f", "Tractora_Simple")
#    di = obtener_entradas_digitales("f", "Tractora_Panico")
#    di = obtener_entradas_digitales("f", "Tractora_Nomasrobos")
#    di = obtener_entradas_digitales("f", "Tractora_ArinTech")
#    di = obtener_entradas_digitales("f", "Tractora_Portavehiculos")
#    di = obtener_entradas_digitales("f", "Tractora_TH14")
#    di = obtener_entradas_digitales("f", "Tractora_TH21")
#    di = obtener_entradas_digitales("f", "Grua_TH21")
#    di = obtener_entradas_digitales("f", "Remolque_TH21")
#    print(di)
    


