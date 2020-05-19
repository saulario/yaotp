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

import re

coordenada_pattern = re.compile("^(?P<grados>\d+):(?P<minutos>\d+):(?P<segundos>\d+\.\d+)(?P<zona>[NSEW])$")

def text2float(coordenada):
    
    retval = None
    match = coordenada_pattern.match(coordenada)
    if (not match):
        return retval;

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