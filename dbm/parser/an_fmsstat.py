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

import parser.safe as safe

class ParserFMSSTATSGET(object):
    
    def __init__(self, context, dispositivo):
        self._context = context
        self._dispositivo = dispositivo
    
    def _eliminar_campos(self, cc):
        cc.pop(0)
        cc.pop(0) 

    def _get_datos(self, mensaje, grupo):
        if not grupo in mensaje:
            mensaje[grupo] = {}
        return mensaje[grupo]    

    def _parse_final(self, mensaje, campos):
        mensaje["final"] = campos.pop(0)

    def _parse_completo(self, mensaje, campos):
        aux = ("%s %s" % (campos.pop(0), campos.pop(0)))
        mensaje["fechaInicio"] = safe.to_datetime(aux, "%d/%m/%Y %H:%M:%S")
        mensaje["distanciaInicio"] = campos.pop(0)
        mensaje["litrosInicio"] = campos.pop(0)
        mensaje["horasInicio"] = campos.pop(0)
        aux = ("%s %s" % (campos.pop(0), campos.pop(0)))
        mensaje["fechaFin"] = safe.to_datetime(aux, "%d/%m/%Y %H:%M:%S")
        mensaje["distanciaFin"] = campos.pop(0)
        mensaje["litrosFin"] = campos.pop(0)
        mensaje["horasFin"] = campos.pop(0)

        rpm = self._get_datos(mensaje, campos.pop(0))
        rpm["range0"] = safe.to_float(campos.pop(0))
        rpm["rangeff"] = safe.to_float(campos.pop(0))
        rpm["range1"] = safe.to_float(campos.pop(0))
        rpm["range2"] = safe.to_float(campos.pop(0))
        rpm["range3"] = safe.to_float(campos.pop(0))
        rpm["range4"] = safe.to_float(campos.pop(0))
        rpm["range5"] = safe.to_float(campos.pop(0))
        rpm["maximo"] = safe.to_float(campos.pop(0))
        rpm["media"] = safe.to_float(campos.pop(0))
        rpm["muestras"] = safe.to_float(campos.pop(0))

        temp = self._get_datos(mensaje, campos.pop(0))
        temp["range0"] = safe.to_float(campos.pop(0))
        temp["rangeff"] = safe.to_float(campos.pop(0))
        temp["range1"] = safe.to_float(campos.pop(0))
        temp["range2"] = safe.to_float(campos.pop(0))
        temp["range3"] = safe.to_float(campos.pop(0))
        temp["range4"] = safe.to_float(campos.pop(0))
        temp["range5"] = safe.to_float(campos.pop(0))
        temp["range6"] = safe.to_float(campos.pop(0))
        temp["maximo"] = safe.to_float(campos.pop(0))
        temp["media"] = safe.to_float(campos.pop(0))
        temp["muestras"] = safe.to_float(campos.pop(0))

        speed = self._get_datos(mensaje, campos.pop(0))
        speed["range0"] = safe.to_float(campos.pop(0))
        speed["rangeff"] = safe.to_float(campos.pop(0))
        speed["range1"] = safe.to_float(campos.pop(0))
        speed["range2"] = safe.to_float(campos.pop(0))
        speed["range3"] = safe.to_float(campos.pop(0))
        speed["range4"] = safe.to_float(campos.pop(0))
        speed["range5"] = safe.to_float(campos.pop(0))
        speed["range6"] = safe.to_float(campos.pop(0))
        speed["range7"] = safe.to_float(campos.pop(0))
        speed["range8"] = safe.to_float(campos.pop(0))
        speed["range9"] = safe.to_float(campos.pop(0))
        speed["maximo"] = safe.to_float(campos.pop(0))
        speed["media"] = safe.to_float(campos.pop(0))
        speed["muestras"] = safe.to_float(campos.pop(0))

        ent = self._get_datos(mensaje, campos.pop(0))
        ent["range0"] = safe.to_float(campos.pop(0))
        ent["rangeff"] = safe.to_float(campos.pop(0))
        ent["range1"] = safe.to_float(campos.pop(0))
        ent["range2"] = safe.to_float(campos.pop(0))
        ent["range3"] = safe.to_float(campos.pop(0))
        ent["range4"] = safe.to_float(campos.pop(0))
        ent["maximo"] = safe.to_float(campos.pop(0))
        ent["media"] = safe.to_float(campos.pop(0))
        ent["muestras"] = safe.to_float(campos.pop(0))

        max_ent = self._get_datos(mensaje, campos.pop(0))
        max_ent["range0"] = safe.to_float(campos.pop(0))
        max_ent["rangeff"] = safe.to_float(campos.pop(0))
        max_ent["range1"] = safe.to_float(campos.pop(0))
        max_ent["range2"] = safe.to_float(campos.pop(0))
        max_ent["range3"] = safe.to_float(campos.pop(0))
        max_ent["range4"] = safe.to_float(campos.pop(0))
        max_ent["range5"] = safe.to_float(campos.pop(0))
        max_ent["range6"] = safe.to_float(campos.pop(0))
        max_ent["range7"] = safe.to_float(campos.pop(0))
        max_ent["range8"] = safe.to_float(campos.pop(0))
        max_ent["range9"] = safe.to_float(campos.pop(0))
        max_ent["range10"] = safe.to_float(campos.pop(0))
        max_ent["maximo"] = safe.to_float(campos.pop(0))
        max_ent["media"] = safe.to_float(campos.pop(0))
        max_ent["muestras"] = safe.to_float(campos.pop(0))

        acc = self._get_datos(mensaje, campos.pop(0))
        acc["range0"] = safe.to_float(campos.pop(0))
        acc["rangeff"] = safe.to_float(campos.pop(0))
        acc["range1"] = safe.to_float(campos.pop(0))
        acc["range2"] = safe.to_float(campos.pop(0))
        acc["range3"] = safe.to_float(campos.pop(0))
        acc["range4"] = safe.to_float(campos.pop(0))
        acc["range5"] = safe.to_float(campos.pop(0))
        acc["range6"] = safe.to_float(campos.pop(0))
        acc["range7"] = safe.to_float(campos.pop(0))
        acc["range8"] = safe.to_float(campos.pop(0))
        acc["range9"] = safe.to_float(campos.pop(0))
        acc["range10"] = safe.to_float(campos.pop(0))
        acc["maximo"] = safe.to_float(campos.pop(0))
        acc["media"] = safe.to_float(campos.pop(0))
        acc["muestras"] = safe.to_float(campos.pop(0))

        if not len(campos):
            return mensaje

        mode = self._get_datos(mensaje, campos.pop(0))
        mode["range0"] = safe.to_float(campos.pop(0))
        mode["rangeff"] = safe.to_float(campos.pop(0))
        mode["range1"] = safe.to_float(campos.pop(0))
        mode["range2"] = safe.to_float(campos.pop(0))
        mode["range3"] = safe.to_float(campos.pop(0))
        mode["range4"] = safe.to_float(campos.pop(0))
        mode["range5"] = safe.to_float(campos.pop(0))
        mode["range6"] = safe.to_float(campos.pop(0))
        mode["range7"] = safe.to_float(campos.pop(0))
        mode["maximo"] = safe.to_float(campos.pop(0))
        mode["media"] = safe.to_float(campos.pop(0))
        mode["muestras"] = safe.to_float(campos.pop(0))

        percent = self._get_datos(mensaje, campos.pop(0))
        percent["range0"] = safe.to_float(campos.pop(0))
        percent["rangeff"] = safe.to_float(campos.pop(0))
        percent["range1"] = safe.to_float(campos.pop(0))
        percent["range2"] = safe.to_float(campos.pop(0))
        percent["range3"] = safe.to_float(campos.pop(0))
        percent["range4"] = safe.to_float(campos.pop(0))
        percent["range5"] = safe.to_float(campos.pop(0))
        percent["range6"] = safe.to_float(campos.pop(0))
        percent["range7"] = safe.to_float(campos.pop(0))
        percent["range8"] = safe.to_float(campos.pop(0))
        percent["range9"] = safe.to_float(campos.pop(0))
        percent["range10"] = safe.to_float(campos.pop(0))
        percent["maximo"] = safe.to_float(campos.pop(0))
        percent["media"] = safe.to_float(campos.pop(0))
        percent["muestras"] = safe.to_float(campos.pop(0))

    def parse(self, texto):
        campos = texto.split(",")
        mensaje = {}
        mensaje["idDispositivo"] = self._dispositivo["ID_MOVIL"]
        mensaje["matricula"] = self._dispositivo["REGISTRATION"]
        mensaje["tipoMensaje"] = "TDI*FMSSTATSGET"  
        mensaje["fechaProceso"] = self._context.ahora

        # mensaje["raw"] = texto

        if self._context.debug == mensaje["idDispositivo"]:
            mensaje["dbgMascara"] = self._dispositivo["maskBin"]
            mensaje["dbgMensaje"] = texto        

        self._eliminar_campos(campos)
        
        aux = campos.pop(0).split("=")
        mensaje["indice"] = safe.to_int(aux[1])

        if "FINAL" not in campos:
            self._parse_completo(mensaje, campos)
        else:
            self._parse_final(mensaje, campos)

        return mensaje