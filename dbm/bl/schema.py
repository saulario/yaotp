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

from sqlalchemy import MetaData, Table

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

    def execute_read(self, conn, stmt):
        return self.to_dict(conn.execute(stmt).fetchone())

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


class PosicionesDAL(BaseDAL):

    def __init__(self, metadata):
        super().__init__(metadata, "Posiciones")

    def delete(self, conn, pk):
        t = self._t
        stmt = t.delete().where(t.c.idMovil == pk)
        conn.execute(stmt)

    def read(self, conn, pk):
        t = self._t
        stmt = t.select().where(t.c.idMovil == pk)
        return self.execute_read(conn, stmt)

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
