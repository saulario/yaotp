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

valor_can_pattern = re.compile("^(?P<b1>[a-f0-9]{2})(?P<b2>[a-f0-9]{2})"
                              + "(?P<b3>[a-f0-9]{2})(?P<b4>[a-f0-9]{2})$")
    
def obtener_temp_motor(v):
    if v is None or v == "f":
        return None
    res = int(v[:2], base=16) - 40
    return res
    
def obtener_temp_fuel(v):
    if v is None or v == "f":
        return None
    res = int(v[2:4], base=16) - 40
    return res
    
def obtener_valor_can(v, k):
    match = valor_can_pattern.match(v)
    if not match:
        return None
    b1 = int(match.group("b1"), base=16)
    b2 = int(match.group("b2"), base=16)
    b3 = int(match.group("b3"), base=16)
    b4 = int(match.group("b4"), base=16)
    res = (b4 << 24) | (b3 << 16) | (b2 << 8) | b1
    res = int(res * k)
    return res    

def obtener_odometro(v):
    return obtener_valor_can(v, 0.005)
    
def obtener_horas(v):
    return obtener_valor_can(v, 0.05)

def obtener_combustible(v):
    return obtener_valor_can(v, 0.5)

def obtener_velocidad(v):
    if v is None or v == "f":
        return None
    v1 = int(v[2:4], base=16)
    v2 = int(v[4:6], base=16)
    v3 = v2 << 8 | v1        
    return round(v3/256)    