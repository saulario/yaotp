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

class ParserFMSSTATGET(object):
    
    def __init__(self, context, dispositivo):
        self._context = context
        self._dispositivo = dispositivo
    
    def _eliminar_campos(self, cc):
        cc.pop(0)
        cc.pop(0) 

    def parse(self, texto):
        campos = texto.split(",")
        mensaje = {}
        mensaje["idDispositivo"] = self._dispositivo["ID_MOVIL"]
        mensaje["matricula"] = self._dispositivo["REGISTRATION"]
        mensaje["tipoMensaje"] = "TDI*FMSSTATGET"  
        mensaje["fechaProceso"] = self._context.ahora

        mensaje["raw"] = texto                  # TODO ELIMINAR!!

        if self._context.debug == mensaje["idDispositivo"]:
            mensaje["dbgMascara"] = self._dispositivo["maskBin"]
            mensaje["dbgMensaje"] = texto        

        self._eliminar_campos(campos)
        
    




        
        return mensaje