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

def obtener_distancia(v):
    if v is None or v == "f":
        return None
    b1 = int(v[0:2], base=16)
    b2 = int(v[2:4], base=16)
    b3 = int(v[4:6], base=16)
    b4 = int(v[6:8], base=16)
    res = (b4 << 24) | (b3 << 16) | (b2 << 8) | b1
    res = int(res * 0.005)
    return res
    
def obtener_temp_motor(v):
    if v is None or v == "f":
        return None
    res = float(v[:2], base=16) - 40
    return res
#CInt("&H" & Mid(temperatura, 1, 2)) - 40
    
def obtener_temp_fuel(v):
    if v is None or v == "f":
        return None
    res = float(v[2:4], base=16) - 40
    return res
#CInt("&H" & Mid(temperatura, 3, 2)) - 40


