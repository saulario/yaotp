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
import pymongo

log = logging.getLogger(__name__)

#
#
#
class Context(object):
    
    def __init__(self, mq):
        """
        Constructor
        """
        self.client = pymongo.MongoClient("mongodb://sdcore:4n2HruMDDz4G1HgKgm6i9I8YaPwTl44DcXN3Wej7u2BbMY7zPcTJD2aN57b66fQtIPcqgKvVPe7lPHxb4YNN9g==@sdcore.documents.azure.com:10255/?ssl=true&replicaSet=globaldb")
        self.mq = mq
        self.db = self.client.get_database("telemetria")
        self._dispositivos = []
        
        self.home = None
        self.url = None
        self.queue = None
        self.user = None
        self.password = None
        
    def close(self):
        """
        Cierre de recursos del contexto
        """
        self.client.close()
        
    def get_dispositivos(self):
        """
        Mantiene una lista de los dispositivos vinculados a la cola
        """
        if len(self._dispositivos) == 0:
            self._dispositivos = dict((d["ID_MOVIL"], d) for d in self.db.dispositivos.find())
        return self._dispositivos
        