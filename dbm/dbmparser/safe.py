#!/usr/bin/python3
# -*- coding: utf-8 -*-

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
