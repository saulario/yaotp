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

def obtener_ebs_haldex(d):
    ebs = {}
    ebs["HALDEX"] = d
    return ebs

def obtener_velocidad_knorr(d):
    return None

def obtener_frenos_knorr(d):
    return None

def obtener_hrvd_knorr(d):
    return None

def obtener_peso_knorr(d):
    return None

def obtener_ebs_knorr(d):
    """
    speed:   b502ffffffffffff
    brakes:  00caffffffffffff
    hrvd:    a8fc6707a8fc6707
    weight:  ffffff4408ffffff
    """
    ebs = {}
    ebs["KNORR"] = d
    v = obtener_velocidad_knorr(d)
    if v:
        ebs["velocidad"] = v
    v = obtener_frenos_knorr(d)          
    if v:
        ebs["frenos"] = v
    v = obtener_hrvd_knorr(d)
    if v:
        ebs["hrvd"] = v
    v = obtener_peso_knorr(d)
    if v:
        ebs["peso"] = 0
    return ebs

def obtener_ebs_wabco(d):
    ebs = {}
    ebs["WABCO"] = d
    return ebs
