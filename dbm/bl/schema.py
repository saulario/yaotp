#!/usr/bin/python3
# -*- coding: utf-8 -*-

from sqlalchemy import and_

import datetime


from sqlalchemy import MetaData, Table

class Entity():
    """
    Entidad para manejar objetos de base de datos
    """
    def __init__(self, proxy):
        """
        Constructor a partir de una fila leída de base de datos
        """
        if proxy is None:
            return
        for key in proxy.iterkeys():
            setattr(self, key, proxy[key])


class BaseDAL():

    def __init__(self, metadata, nombre):
        self._metadata = metadata
        self._t = Table(nombre, metadata, autoload = True)


    def to_dict(self, proxy):
        if proxy is None:
            return None
        row = {}
        for key in proxy.iterkeys():
            row[key] = proxy[key]
        return row


    def insert(self, conn, **kwargs):
        t = self._t
        stmt = t.insert().values(kwargs)
        return conn.execute(stmt)


    def query(self, conn, stmt):
        retval = []
        results = conn.execute(stmt).fetchall()
        for result in results:
            retval.append(self.to_dict(result))
        return retval


    def execute_read_viejo(self, conn, stmt):
        return self.to_dict(conn.execute(stmt).fetchone())


    def execute_read(self, conn, stmt):
        return Entity(conn.execute(stmt).fetchone())


    def select(self):
        return self._t.select()


    def get_table(self):
        return self._t


class FMSStatsDAL(BaseDAL):

    def __init__(self, metadata):
        super().__init__(metadata, "FMSStats")

    def delete(self, conn, pk):
        t = self._t
        stmt = t.delete().where(t.c.IdMovil == pk)
        conn.execute(stmt)

    def read(self, conn, pk):
        t = self._t
        stmt = t.select().where(t.c.IdMovil == pk)
        return self.execute_read(conn, stmt)

    def update(self, conn, pk, **kwargs):
        t = self._t
        stmt = t.update().values(kwargs).where(t.c.IdMovil == pk)
        conn.execute(stmt) 

    def getInstance(self, mensaje):
        retval = {}
        retval["idMovil"] = mensaje["idDispositivo"]
        retval["indice"] = mensaje["indice"] 

        return retval


class MovilDAL(BaseDAL):

    def __init__(self, metadata):
        super().__init__(metadata, "Movil")

    def delete(self, conn, pk):
        t = self._t
        stmt = t.delete().where(t.c.IdMovil == pk)
        conn.execute(stmt)

    def read(self, conn, pk):
        t = self._t
        stmt = t.select().where(t.c.IdMovil == pk)
        return self.execute_read(conn, stmt)

    def update(self, conn, pk, **kwargs):
        t = self._t
        stmt = t.update().values(kwargs).where(t.c.IdMovil == pk)
        conn.execute(stmt)        


class PosicionesPK():
    
    def __init__(self, idmovil = None, FechaHoraGPS = None):
        self.idmovil = idmovil
        self.FechaHoraGPS = FechaHoraGPS


class PosicionesDAL(BaseDAL):

    def __init__(self, metadata):
        super().__init__(metadata, "Posiciones")


    def delete(self, conn, pk):
        t = self._t
        stmt = t.delete().where(and_(
                t.c.idmovil == pk.idmovil,
                t.c.FechaHoraGPS == pk.FechaHoraGPS
        ))
        conn.execute(stmt)


    def read(self, conn, pk):
        t = self._t
        stmt = t.select().where(and_(
                t.c.idmovil == pk.idmovil,
                t.c.FechaHoraGPS == pk.FechaHoraGPS
        ))
        return self.execute_read(conn, stmt)


    def update(self, conn, pk, **kwargs):
        t = self._t
        stmt = t.update().values(kwargs).where(and_(
                t.c.idmovil == pk.idmovil,
                t.c.FechaHoraGPS == pk.FechaHoraGPS
        ))
        conn.execute(stmt)        


    def _get_instance_CANBUS(self, retval, mensaje):
        if "CANBUS" not in mensaje:
            return

    def _get_instance_DI(self, retval, mensaje):
        if "DI" not in mensaje:
            return

    def _get_instance_GPS(self, retval, mensaje):
        if "GPS" not in mensaje:
            return
        retval["latitud"] = mensaje["GPS"]["posicion"]["coordinates"][0]
        retval["Longitud"] = mensaje["GPS"]["posicion"]["coordinates"][1]
        retval["Altitud"] = mensaje["GPS"]["altitud"]
        retval["NumeroSatelites"] = mensaje["GPS"]["satelites"]        
        retval["Velocidad"] = mensaje["GPS"]["velocidad"]
        retval["Direccion"] = mensaje["GPS"]["rumbo"]
        retval["FechaHoraGPS"] = mensaje["GPS"]["fecha"]
        retval["Kilometros"] = mensaje["GPS"]["kilometros"]
    
    def _get_instance_GSM(self, retval, mensaje):
        if "GSM" not in mensaje:
            return
        retval["CalidadGSM"] = mensaje["GSM"]["calidadSenal"]

    def getInstance(self, mensaje):
        retval = {}
        retval["FechaHora"] = mensaje["fecha"]
        retval["idmovil"] = mensaje["idDispositivo"]
        retval["FechaHoraGPS"] = mensaje["fecha"] # se sobreescribe si viene información GPS

        self._get_instance_CANBUS(retval, mensaje)
        self._get_instance_DI(retval, mensaje)
        self._get_instance_GPS(retval, mensaje)
        self._get_instance_GSM(retval, mensaje)

        return retval
