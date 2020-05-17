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

import datetime
import logging

log = logging.getLogger(__name__)

def divide(num, den):
    try:
        return num / den
    except TypeError:
        log.debug("\tDivisión inválida %s / %s" % (num, den))
        return None

def to_float(valor, digitos = None):
    retval = None
    try:
        retval = float(valor)
    except ValueError:
        log.debug("\tRecibido valor no convertible a float %s" % (valor))
        return retval
    if digitos is not None:
        retval = round(retval, to_int(digitos))
    return retval

def to_int(valor):
    try:
        return int(valor)
    except ValueError:
        log.debug("\tRecibido valor no convertible a int %s" % (valor))
        return None

def to_datetime(valor, formato):
    try:
        return datetime.datetime.strptime(valor, formato)
    except ValueError:
        return None
