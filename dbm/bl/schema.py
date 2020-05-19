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

    def update(self):
        return self._t.update()

    def delete(self):
        return self._t.delete()

    
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
