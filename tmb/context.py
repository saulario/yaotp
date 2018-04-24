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

from datetime import datetime

class Context(object):
    
    def __init__(self):
        """
        Constructor
        """
        self.debug = -1
        self.client = None
        self.db = None
        self._dispositivos = None
        
        self.home = None
        self.url = None
        self.queue = None
        self.user = None
        self.password = None
        self.ahora = datetime.utcnow()
              
    def get_dispositivos(self):
        """
        Mantiene una lista de los dispositivos vinculados a la cola
        """
        if self._dispositivos is None:
            self._dispositivos = dict((d["ID_MOVIL"], d) for d in self.db.dispositivos.find())
        return self._dispositivos
        