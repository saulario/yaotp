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

log = logging.getLogger(__name__)

tacografo_pattern = re.compile(("^(?P<datos>[a-z0-9]{16})"
                                + "(?P<pais1>[A-Z\s]{0,3})"
                                + "(?P<cond1>.{0,16})\*"
                                + "(?P<pais2>[A-Z\s]{0,3})"
                                + "(?P<cond2>.{0,16})\*$"))

def componer_respuesta(v, t):
#    return { "estado": v, "texto": t}
    return t

def tarjeta_presente(v):
    switch = {
            0: "Card not present",
            1: "Card present",
            }    
    return componer_respuesta(v, switch.get(v, "Unknown")) 

def conductor_estado(v):
    switch = {
            0: "Rest",
            1: "Driver available",
            2: "Work",
            3: "Drive",
            7: "Not available"
            }
    return componer_respuesta(v, switch.get(v, "Unknown"))

def conductor_alarma(v):
    switch = {
            0: "Normal",
            1: "15 min before 4 1/2 h",
            2: "4 1/2 h reached",
            3: "15 min before 9 h",
            4: "9 h reached",
            5: "15 min before 16 h",
            7: "No available"
            }    
    return componer_respuesta(v, switch.get(v, "Unknown"))

def vehiculo_movimiento(v):
    switch = {
            0: "Not detected",
            1: "Detected",
            2: "Error",
            3: "Not available"
            }    
    return componer_respuesta(v, switch.get(v, "Unknown"))

def exceso_velocidad(v):
    switch = {
            0: "No overspeed",
            1: "Overspeed",
            2: "Error",
            3: "Not available"
            }    
    return componer_respuesta(v, switch.get(v, "Unknown"))

def evento(v):
    switch = {
            0: "No tachograph event",
            1: "Tachograph event",
            2: "Error",
            3: "Not available"
            }    
    return componer_respuesta(v, switch.get(v, "Unknown"))   

def manipulacion(v):
    switch = {
            0: "No handling information",
            1: "Handling information",
            2: "Error",
            3: "Not available"
            }    
    return componer_respuesta(v, switch.get(v, "Unknown"))  

def modo(v):
    switch = {
            0: "Normal performance",
            1: "Performance analysis",
            2: "Error",
            3: "Not available"
            }    
    return componer_respuesta(v, switch.get(v, "Unknown")) 

def sentido(v):
    switch = {
            0: "Fordward",
            1: "Reverse",
            2: "Error",
            3: "Not available"
            }    
    return componer_respuesta(v, switch.get(v, "Unknown")) 

def obtener_cond1_presente(texto):
    v = int(texto[2], base=16) & 3
    return tarjeta_presente(v)
#(Right$(ConvBinario(Mid$(tacografo, 3, 1)), 2)) --
    
def obtener_cond2_presente(texto):
    v = int(texto[4], base=16) & 3
    return tarjeta_presente(v)
#(Right$(ConvBinario(Mid$(tacografo, 5, 1)), 2)) --

def obtener_cond1_estado(texto):
    v = int(texto[1], base=16) & 7
    return conductor_estado(v)
#Right$(ConvBinario(Mid$(tacografo, 2, 1)), 3)) --

def obtener_cond2_estado(texto):
    z1 = (int(texto[0], base=16) & 3) << 1
    z2 = (int(texto[1], base=16) & 8) >> 3
    v = z1 | z2
    return conductor_estado(v)
#(Right$(ConvBinario(Left$(tacografo, 1)), 2)) & 
#Trim(Left$(ConvBinario(Mid$(tacografo, 2, 1)), 1))

def obtener_cond1_alarma(texto):
    v = int(texto[3], base=16)
    return conductor_alarma(v)
#(Trim(ConvBinario(Mid$(tacografo, 4, 1))))

def obtener_cond2_alarma(texto):
    v = int(texto[5], base=16)
    return conductor_alarma(v)
#(Trim(ConvBinario(Mid$(tacografo, 6, 1))))

def obtener_movimiento(texto):
    v = int(texto[0], base=16) >> 2
    return vehiculo_movimiento(v)
#Left$(ConvBinario(Left$(tacografo, 1)), 2))

def obtener_exceso_velocidad(texto):
    v = int(texto[2], base=16) >> 2
    return exceso_velocidad(v)
#(Left$(ConvBinario(Mid$(tacografo, 3, 1)), 2))

def obtener_evento(texto):
    v = int(texto[7], base=16) & 3
    return evento(v)
#(Right$(ConvBinario(Mid$(tacografo, 8, 1)), 2))

def obtener_manipulacion(texto):
    v = int(texto[7], base=16) >> 2
    return manipulacion(v)
#(Left$(ConvBinario(Mid$(tacografo, 8, 1)), 2))

def obtener_modo(texto):
    v = int(texto[6], base=16) & 3
    return modo(v)
#(Right$(ConvBinario(Mid$(tacografo, 7, 1)), 2))

def obtener_sentido(texto):
    v = int(texto[6], base=16) >> 2
    return sentido(v)
#(Left$(ConvBinario(Mid$(tacografo, 7, 1)), 2))

def obtener_datos_tacografo(texto):
    log.debug("-----> Inicio")
    log.debug("\t(texto): %s" % texto)
    
    if texto is None or len(texto) < 16:
        log.debug("<----- Salida, no hay datos")
        return None
    match = tacografo_pattern.match(texto)
    if not match:
        log.debug("<----- Salida, no coincide el patron")
        return None    
    
    tacho = {}
    t = match.group("cond1")
    if len(t) > 0:
        tacho["c1Tarjeta"] = t
        tacho["c1Estado"] = obtener_cond1_estado(texto)
        tacho["c1Alarma"] = obtener_cond1_alarma(texto)            
    tacho["c1Presente"] = obtener_cond1_presente(texto)
    
    t = match.group("cond2")
    if len(t) > 0:
        tacho["c2Tarjeta"] = t
        tacho["c2Estado"] = obtener_cond2_estado(texto)
        tacho["c2Alarma"] = obtener_cond2_alarma(texto)        
    tacho["c2Presente"] = obtener_cond2_presente(texto)    
    
    tacho["movimiento"] = obtener_movimiento(texto)
    tacho["excesoVelocidad"] = obtener_exceso_velocidad(texto)
    tacho["evento"] = obtener_evento(texto)
    tacho["manipulacion"] = obtener_manipulacion(texto)
    tacho["modo"] = obtener_modo(texto)
    tacho["sentido"] = obtener_sentido(texto)

    log.debug("<----- Fin")
    return tacho        