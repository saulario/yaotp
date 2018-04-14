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
                                + "(?P<pais>[A-Z\s]{0,3})"
                                + "(?P<cond1>.{0,16})\*"
                                + "(?P<cond2>.{0,16})\*$"))

def componer_respuesta(v, t):
#    return { "estado": v, "texto": t}
    return t

def conductor_estado(v):
    switch = {
            0: "Rest",
            1: "Driver available",
            2: "Work",
            3: "Drive",
            7: "Not available"
            }
    return componer_respuesta(v, switch.get(v, "Unknown"))

def conduccion_estado(v):
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
            0: "Detected",
            1: "Not detected",
            3: "Not available"
            }    
    return componer_respuesta(v, switch.get(v, "Unknown"))

def obtener_cond1_estado(texto):
    v = int(texto[1], base=16) & 7
    return conductor_estado(v)

def obtener_cond2_estado(texto):
    z1 = bin(int(texto[0], base=16) & 3)[2:]
    z2 = bin(int(texto[3], base=16) & 1)[2:]
    v = int(z1 + z2, base=2)
    return conductor_estado(v)

def obtener_movimiento(texto):
    v = int(texto[0], base=16) & 3
    return vehiculo_movimiento(v)

def obtener_datos_tacografo(texto):
    log.info("-----> Inicio")
    log.info("\t(texto): %s" % texto)
    
    if texto is None or len(texto) < 16:
        log.info("<----- Salida, no hay datos")
        return None
    match = tacografo_pattern.match(texto)
    if not match:
        log.info("<----- Salida, no coincide el patron")
        return None    
    
    tacho = {}
    tacho["c1Tarjeta"] = match.group("cond1")
    tacho["c2Tarjeta"] = match.group("cond2")
    tacho["c1Estado"] = obtener_cond1_estado(texto)
#    tacho["c1Alarma"] = obtener_cond1_alarma(texto)
    tacho["c2Estado"] = obtener_cond2_estado(texto)
#    tacho["c2Alarma"] = obtener_cond2_alarma(texto)
    tacho["movimiento"] = obtener_movimiento(texto)

    log.info("<----- Fin")
    return tacho