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
import re

import canbus
import gis
import tacografo as taco

log = logging.getLogger(__name__)

tacografo_pattern = re.compile(("^(?P<datos>[a-z0-9]{16})"
                                + "(?P<pais>[A-Z\s]{0,3})"
                                + "(?P<cond1>.{0,16})\*"
                                + "(?P<cond2>.{0,16})\*$"))

#
#
#
class Mensaje(object):
    """
    Clase base para todos los mensajes recibidos
    """
    def __init__(self, id_movil, registro):
        self.id_movil = id_movil
        self.registro = registro
    
#
# Parser nulo
#
class ParserNULL(object):
    """
    OTROS
    """
    
    def parse(self, mensaje):
        return None

#
# TDI*GETDRIVERINFO
#
class ParserGETDRIVERINFO(object):
    """
    TDI*GETDRIVERINFO
    """
    
    def __init__(self, context):
        self._context = context
    
    def parse(self, message):
        return None   

#
# TDI*P
#
class ParserP(object):
    """
    TDI *P
    """
    IDENTIFICADOR = 1
    FECHA_HORA = 2
    FECHA_HORA_1 = 3   
    DATOS_GPS = 4
    PT100_EXTERNAS = 5
    ENTRADAS_ANALOGICAS = 6
    ENTRADAS_DIGITALES_EXTENDIDAS = 7
    IBOX = 8
    CARRIER = 9
    CONTADOR = 10
    PANTALLA = 11
    CONDUCTOR = 12
    CANBUS = 13
    DL_7080 = 14
    INFORMACION_GSM = 15
    PALFINGER = 16
    SOCKET = 17
    MASTER = 18
    NS_CARRIER = 19
    DAS= 20
    TRANSCAN = 21
    
    CANBUS_FUEL = 23
    BLOQUE_ALMACENAMIENTO = 24
    TH12_ONLINE = 25
    LECTOR_DE_TARJETAS = 26
    CANBUS_HORAS = 27
    DOBLE_CONDUCTOR = 28
    
    ALARMA_PUERTA_SLAVE = 31
    EUROSCAN = 32
    DATACOLD = 33
    CANBUS_EXTENDIDO = 34
    THERMO_GUARD_VI = 35
    DL_7080_TRAILER = 36
    SALIDAS_DIGITALES = 37
    IBUTTON = 38
    PT100_INTERNAS = 39
    KNORR = 40
    WABCO = 41
    HALDEX = 42
    GLP_IVECO_EURO5 = 43
    CANBUS_FMS3 = 44
    TOUCHPRINT= 45
    POWER_SUPPLY = 46
    
    LLS20160 = 48
    INTELLISET = 49
    TH16 = 50

    def __init__(self, context, dispositivo):
        """
        Constructor
        """
        self._context = context
        self._dispositivo = dispositivo
        self._mascara = list(x for x in (dispositivo["MASK"]))
        self._mascara.insert(0, "X")                                # fuerza base 1

    def _get_datos_canbus(self, mensaje):
        if not "CANBUS" in mensaje:
            mensaje["CANBUS"] = {}
        return mensaje["CANBUS"]
    
    def _get_datos_conductores(self, mensaje):
        if not "COND" in mensaje:
            mensaje["COND"] = {}
        return mensaje["COND"]    
        
    def _get_datos_gps(self, mensaje):
        if not "GPS" in mensaje:
            mensaje["GPS"] = {}
        return mensaje["GPS"]
    
    def _get_datos_temperatura(self, mensaje):
        if not "TEMP" in mensaje:
            self._td = {}
            self._td["sondas"] = []
            mensaje["TEMP"] = self._td
        return mensaje["TEMP"]

    def _get_datos_7080(self, mensaje):
        if not "7080" in mensaje:
            mensaje["7080"] = {}
        return mensaje["7080"]
        
    def _eliminar_campos(self, cc):
        cc.pop(0)
        cc.pop(0) 
        
    def _01_identificador(self, mm, campos, mensaje):
        self._current = int(mm[self.IDENTIFICADOR])
        if not self._current:
            return
        campos.pop(0)
         
    def _02_fecha_hora(self, mm, campos, mensaje):
        self._current = int(mm[self.FECHA_HORA])
        if not self._current:
            return
        self._aux = ("%s %s" % (campos.pop(0), campos.pop(0)))
        mensaje["fecha"] = datetime.datetime.strptime(self._aux, "%d/%m/%Y %H:%M:%S")
        
    def _03_fecha_hora1(self, mm, campos, mensaje):
        self._current = int(mm[self.FECHA_HORA_1])
        if not self._current:
            return
        self._aux = ("%s %s" % (campos.pop(0), campos.pop(0)))
        self._d = self._get_datos_gps(mensaje)
        self._d["fecha"] = datetime.datetime.strptime(self._aux, "%d/%m/%Y %H:%M:%S")

    def _04_datos_gps(self, mm, campos, mensaje):
        self._current = int(mm[self.DATOS_GPS])
        if not self._current:
            return
        self._d = self._get_datos_gps(mensaje)
        self._d["posicion"] = gis.convertir_coordenada_GPS(campos.pop(0), campos.pop(0))
        self._d["altitud"] = float(campos.pop(0))
        self._d["velocidad"] = float(campos.pop(0))
        self._d["rumbo"] = int(campos.pop(0))
        self._d["satelites"] = int(campos.pop(0))

    def _05_pt100_internas(self, mm, campos, mensaje):
        self._current = int(mm[self.PT100_INTERNAS])
        if not self._current:
            return       
        self._d = self._get_datos_temperatura(mensaje)
        self._sondas = list(float(campos.pop(0)) for i in range(3 + int(mm[self.TH16])))
        for t in self._sondas:
            self._d["sondas"].append(t)
        
    def _06_pt100_externas(self, mm, campos, mensaje):
        self._current = int(mm[self.PT100_EXTERNAS])
        if not self._current:
            return
        self._d = self._get_datos_temperatura(mensaje)        
        self._sondas = list(float(campos.pop(0)) for i in range(3))
        for t in self._sondas:
            self._d["sondas"].append(t)        
        
    def _07_entradas_analogicas(self, mm, campos, mensaje):
        self._current = int(mm[self.ENTRADAS_ANALOGICAS])
        if not self._current:
            return
        mensaje["analogicInput"] = list(float(campos.pop(0)) for i in range(int(campos.pop(0))))
        
    def _08_transcan(self, mm, campos, mensaje):
        self._current = int(mm[self.TRANSCAN])
        if not self._current:
            return        
        self._d = self._get_datos_temperatura(mensaje)        
        self._sondas = list(float(campos.pop(0)) for i in range(campos.pop(0)))
        for t in self._sondas:
            self._d["sondas"].append(t)
        
    def _09_euroscan(self, mm, campos, mensaje):
        self._current = int(mm[self.EUROSCAN])
        if not self._current:
            return        
        self._d = self._get_datos_temperatura(mensaje)        
        self._sondas = list(float(campos.pop(0)) for i in range(5))
        for t in self._sondas:
            self._d["sondas"].append(t)
                
    def _10_datacold(self, mm, campos, mensaje):
        self._current = int(mm[self.DATACOLD])
        if not self._current:
            return        
        self._d = self._get_datos_temperatura(mensaje)        
        self._sondas = list(float(campos.pop(0)) for i in range(4))
        for t in self._sondas:
            self._d["sondas"].append(t)
        self._alarmas_sondas = campos.pop(0)        # ignorado
        self._entradas_digitales = campos.pop(0)    # ignorado
        self._alarmas_eedd = campos.pop(0)          # ignorado
        
    def _fromTouchprintToCelsius(self, t):
        self._temp = float(t) / 10
        self._temp = round(((self._temp -32) / 1.8), 1)
        if self._temp < -1000:
            self._temp = None
        return self._temp

    def _11_touchprint(self, mm, campos, mensaje):
        self._current = int(mm[self.TOUCHPRINT])
        if not self._current:
            return        
        self._d = self._get_datos_temperatura(mensaje)
        self._sondas = list(self._fromTouchprintToCelsius(campos.pop(0)) for i in range(6))
        for t in self._sondas:
            self._d["sondas"].append(t)
        
    def _12_digitales(self, mm, campos, mensaje):
        self._current = int(mm[self.ENTRADAS_DIGITALES_EXTENDIDAS])
        if not self._current:
            return   
        mensaje["entradasDigitales"] = list(campos.pop(0) for i in range(2))
             
    def _13_ibox(self, mm, campos, mensaje):
        self._current = int(mm[self.IBOX])
        if not self._current:
            return        
        self._d = self._get_datos_temperatura(mensaje)
        self._d["alarmas"] = int(campos.pop(0))
        self._d["tempRetorno"] = float(campos.pop(0))
        self._d["tempSuministro"] = float(campos.pop(0))
        self._d["setPoint"] = float(campos.pop(0))
        self._d["horasElectrico"] = float(campos.pop(0))
        self._d["horasMotor"] = float(campos.pop(0))
        self._d["horasTotales"] = float(campos.pop(0))
        self._d["modoOperacion"] = int(campos.pop(0))
             
    def _14_carrier(self, mm, campos, mensaje):
        self._current = int(mm[self.CARRIER])
        if not self._current:
            return        
        self._d = self._get_datos_temperatura(mensaje)
        self._d["setPoint"] = float(campos.pop(0)) / 10
        self._d["modoOperacion"] = list(campos.pop(0) for i in range(2))
        self._d["tempRetorno"] = float(campos.pop(0)) / 10
        self._d["tempSuministro"] = float(campos.pop(0)) / 10
        self._d["presion"] = float(campos.pop(0)) / 10
        self._d["horasTotales"] = float(campos.pop(0))

    def _15_das(self, mm, campos, mensaje):
        self._current = int(mm[self.DAS])
        if not self._current:
            return        
        self._d = self._get_datos_temperatura(mensaje)
        self._d["setPoint"] = float(campos.pop(0))
        self._d["tempRetorno"] = float(campos.pop(0))
        self._d["tempSuministro"] = float(campos.pop(0))
        
    def _16_thermo_guard_vi(self, mm, campos, mensaje):
        self._current = int(mm[self.THERMO_GUARD_VI])
        if not self._current:
            return
        self._d = self._get_datos_temperatura(mensaje)
        self._d["setPoint"] = self._fromTouchprintToCelsius(float(campos.pop(0)))
        self._d["tempRetorno"] = self._fromTouchprintToCelsius(float(campos.pop(0)))
        self._d["tempSuministro"] = self._fromTouchprintToCelsius(float(campos.pop(0)))
        
    def _17_th12online(self, mm, campos, mensaje):
        self._current = int(mm[self.TH12_ONLINE])
        if not self._current:
            return    
        self._d = self._get_datos_temperatura(mensaje)
        self._t = float(campos.pop(0))
        if self._t <= -30 or self._t >= 50:
            self._t = None        
        self._d["sondas"].append(self._t)
                    
    def _18_datos_gps(self, mm, campos, mensaje):
        self._current = int(mm[self.DATOS_GPS])
        if not self._current:
            return
        self._d = self._get_datos_gps(mensaje)
        self._d["kilometros"] = float(campos.pop(0)) 
        self._d["entradasDigitales"] = float(campos.pop(0))

    def _19_contador(self, mm, campos, mensaje):
        self._current = int(mm[self.CONTADOR])
        if not self._current:
            return    
        mensaje["kilometros"] = float(campos.pop(0))        

    def _20_mantenimiento(self, mm, campos, mensaje):
        self._current = int(mm[self.PANTALLA])
        self._aux = int(mm[self.LECTOR_DE_TARJETAS])
        if (not self._current and not self._aux):
            return    
        mensaje["mantenimiento"] = campos.pop(0)
        
    def _21_canbus(self, mm, campos, mensaje):
        self._current = int(mm[self.CANBUS])
        if not self._current:
            return    
        self._d = self._get_datos_canbus(mensaje)
        self._d["tacografo"] = campos.pop(0)
        v = taco.obtener_datos_tacografo(self._d["tacografo"])
        if not v is None:
            mensaje["TACHO"] = v
        v = canbus.obtener_distancia(campos.pop(0))
        if not v is None:
            self._d["distancia"] = v
        v = canbus.obtener_temperatura(campos.pop(0))
        if not v is None:
            self._d["temperatura"] = v

    def _22_canbus_horas(self, mm, campos, mensaje):
        self._current = int(mm[self.CANBUS_HORAS])
        if not self._current:
            return    
        self._d = self._get_datos_canbus(mensaje)
        self._d["horas"] = campos.pop(0)
        
    def _23_canbus_fuel(self, mm, campos, mensaje):
        self._current = int(mm[self.CANBUS_FUEL])
        if not self._current:
            return    
        self._d = self._get_datos_canbus(mensaje)
        self._d["combustible"] = campos.pop(0)

    def _24_canbus_extendido(self, mm, campos, mensaje):
        self._current = int(mm[self.CANBUS_EXTENDIDO])
        if not self._current:
            return    
        self._d = self._get_datos_canbus(mensaje)
        self._d["controlVelocidad"] = campos.pop(0)
        self._d["encendidoElectronico2"] = campos.pop(0)
        self._d["encendidoElectronico1"] = campos.pop(0)
        self._d["fecha"] = campos.pop(0)
        self._d["display"] = campos.pop(0)
        self._d["peso"] = campos.pop(0)

    def _25_canbus_fms3(self, mm, campos, mensaje):
        self._current = int(mm[self.CANBUS_FMS3])
        if not self._current:
            return    
        self._d = self._get_datos_canbus(mensaje)
        self._d["lfe"] = campos.pop(0)
        self._d["erc1"] = campos.pop(0)
        self._d["dc1"] = campos.pop(0)
        self._d["dc2"] = campos.pop(0)
        self._d["etc2"] = campos.pop(0)
        self._d["asc4"] = campos.pop(0)
                
    def _26_glp_iveco_euro5(self, mm, campos, mensaje):
        self._current = int(mm[self.GLP_IVECO_EURO5])
        if not self._current:
            return
        self._d = {}
        self._d["mp3msg1"] = campos.pop(0)
        self._d["mp3msg2"] = campos.pop(0)
        mensaje["IVECO"] = self._d
            
    def _27_knorr(self, mm, campos, mensaje):
        self._current = int(mm[self.KNORR])
        if not self._current:
            return
        self._d = {}
        self._d["hrdv"] = campos.pop(0)
        self._d["speed"] = campos.pop(0)
        self._d["weight"] = campos.pop(0)
        self._d["brakes"] = campos.pop(0)
        self._d["ebsprop1"] = campos.pop(0)
        self._d["ebsprop4"] = campos.pop(0)
        self._d["ebsprop5"] = campos.pop(0)
        self._d["ebsprop6"] = campos.pop(0)
        self._d["ebsprop7"] = campos.pop(0)
        self._d["ebsprop8"] = campos.pop(0)
        self._d["ebsprop9"] = campos.pop(0)                                        
        mensaje["KNORR"] = self._d
            
    def _28_haldex(self, mm, campos, mensaje):
        self._current = int(mm[self.HALDEX])
        if not self._current:
            return
        self._d = {}
        self._d["speed"] = campos.pop(0)
        self._d["pressures"] = campos.pop(0)
        self._d["odometer"] = campos.pop(0)
        self._d["3m"] = campos.pop(0)
        mensaje["HALDEX"] = self._d
            
    def _29_wabco(self, mm, campos, mensaje):
        self._current = int(mm[self.WABCO])
        if not self._current:
            return
        self._d = {}
        self._d["ebs11"] = campos.pop(0)
        self._d["ebs12"] = campos.pop(0)
        self._d["ebs21"] = campos.pop(0)
        self._d["ebs22"] = campos.pop(0)
        self._d["ebs23"] = campos.pop(0)
        self._d["ebs24"] = campos.pop(0)
        self._d["ebs25"] = campos.pop(0)
        self._d["rge11"] = campos.pop(0)
        self._d["rge21"] = campos.pop(0)
        self._d["rge22"] = campos.pop(0)
        self._d["rge23"] = campos.pop(0)
        self._d["hrvd"] = campos.pop(0)
        mensaje["WABCO"] = self._d;
            
    def _30_7080(self, mm, campos, mensaje):
        self._current = int(mm[self.DL_7080])
        if not self._current:
            return
        self._d = self._get_datos_7080(mensaje)    
        self._d["kilometros"] = float(campos.pop(0))
        self._d["velocidad"] = float(campos.pop(0))
        self._["rpm"] = float(campos.pop(0))
        
    def _31_informacion_gsm(self, mm, campos, mensaje):
        self._current = int(mm[self.INFORMACION_GSM])
        if not self._current:
            return
        self._d = {}    
        self._current = int(mm[self.SOCKET])
        if not self._current:
            self._d["operador"] = campos.pop(0)
        self._d["calidadSenal"] = campos.pop(0)
        mensaje["GSM"] = self._d
                
    def _32_id_movil_slave(self, mm, campos, mensaje):
        pass
    
    def _33_master(self, mm, campos, mensaje):
        self._current = int(mm[self.MASTER])
        if not self._current:
            return
        raise RuntimeError("33_master no implementado")
                    
    def _34_trailer_7080(self, mm, campos, mensaje):
        self._current = int(mm[self.DL_7080_TRAILER])
        if not self._current:
            return            
        self._d = self._get_datos_7080(mensaje)
        self._d["idEsclavo"] = campos.pop(0)
        
    def _35_ibutton(self, mm, campos, mensaje):
        self._current = int(mm[self.IBUTTON])
        if not self._current:
            return            
        mensaje["IBUTTON"] = campos.pop(0)
        
    def _36_palfinger(self, mm, campos, mensaje):
        self._current = int(mm[self.PALFINGER])
        if not self._current:
            return
        mensaje["PALFINGER"] = campos.pop(0)
                    
    def _37_ns_carrier(self, mm, campos, mensaje):
        self._current = int(mm[self.NS_CARRIER])
        if not self._current:
            return
        self._d = self._get_datos_temperatura(mensaje)
        self._d["numeroSerie"] = campos.pop(0)
                    
    def _38_conductor(self, mm, campos, mensaje):
        self._current = int(mm[self.CONDUCTOR])
        if not self._current:
            return
        self._d = self._get_datos_conductores(mensaje)
        self._d["id1"] = campos.pop(0)
                    
    def _39_doble_conductor(self, mm, campos, mensaje):
        self._current = int(mm[self.DOBLE_CONDUCTOR])
        if not self._current:
            return
        self._d = self._get_datos_conductores(mensaje)
        self._d["id1"] = campos.pop(0)        
        self._d["id2"] = campos.pop(0)
                    
    def _40_alarma_puerta_slave(self, mm, campos, mensaje):
        self._current = int(mm[self.ALARMA_PUERTA_SLAVE])
        if not self._current:
            return
        mensaje["alarmaPuertaSlave"] = campos.pop(0)

    def _41_lls20160(self, mm, campos, mensaje):
        self._current = int(mm[self.LLS20160])
        if not self._current:
            return
        self._d = {}
        self._d["tempSonda1"] = float(campos.pop(0))
        self._d["tempSonda2"] = float(campos.pop(0))
        self._d["fuelSonda1"] = float(campos.pop(0))
        self._d["fuelSonda1"] = float(campos.pop(0))
        mensaje["LLS20160"] = self._d
                    
    def _42_salidas_digitales(self, mm, campos, mensaje):
        self._current = int(mm[self.SALIDAS_DIGITALES])
        if not self._current:
            return
        mensaje["salidasDigitales"] = campos.pop(0)
                    
    def _43_bloque_almacenamiento(self, mm, campos, mensaje):
        self._current = int(mm[self.BLOQUE_ALMACENAMIENTO])
        if not self._current:
            return
        mensaje["salidasDigitales"] = campos.pop(0)
                    
    def _44_power_supply(self, mm, campos, mensaje):
        self._current = int(mm[self.POWER_SUPPLY])
        if not self._current:
            return
        mensaje["alimentacion"] = float(campos.pop(0))
                    
    def _45_intelliset(self, mm, campos, mensaje):
        self._current = int(mm[self.INTELLISET])
        if not self._current:
            return            
        self._d = {}
        self._d["installed"] = campos.pop(0)
        self._d["loadedNumber"] = campos.pop(0)
        self._d["validForModel"] = campos.pop(0)
        self._d["state"] = campos.pop(0)
        self._d["name"] = campos.pop(0)
        mensaje["INTELLISET"] = self._d
        
    def _99_finales(self, mm, campos, mensaje):        
        mensaje["segmento"] = campos.pop(0)
        mensaje["offset"] = campos.pop(0)
        
    # 11110000001000101001000000000000000000000100111000
    def parse(self, texto):
        self._campos = texto.split(",")
        self._mensaje = {}
        self._mensaje["idDispositivo"] = self._dispositivo["ID_MOVIL"]
        self._mensaje["matricula"] = self._dispositivo["REGISTRATION"]
        self._mensaje["tipoMensaje"] = "TDI*P"
        
        self._mm = self._mascara       
        self._eliminar_campos(self._campos)
        
        self._01_identificador(self._mm, self._campos, self._mensaje)
        self._02_fecha_hora(self._mm, self._campos, self._mensaje)
        self._03_fecha_hora1(self._mm, self._campos, self._mensaje)
        self._04_datos_gps(self._mm, self._campos, self._mensaje)
        self._05_pt100_internas(self._mm, self._campos, self._mensaje) 
        
        self._06_pt100_externas(self._mm, self._campos, self._mensaje)
        self._07_entradas_analogicas(self._mm, self._campos, self._mensaje)
        self._08_transcan(self._mm, self._campos, self._mensaje)
        self._09_euroscan(self._mm, self._campos, self._mensaje)
        self._10_datacold(self._mm, self._campos, self._mensaje)
        
        self._11_touchprint(self._mm, self._campos, self._mensaje)
        self._12_digitales(self._mm, self._campos, self._mensaje)
        self._13_ibox(self._mm, self._campos, self._mensaje)
        self._14_carrier(self._mm, self._campos, self._mensaje)
        self._15_das(self._mm, self._campos, self._mensaje)
        
        self._16_thermo_guard_vi(self._mm, self._campos, self._mensaje)
        self._17_th12online(self._mm, self._campos, self._mensaje)
        self._18_datos_gps(self._mm, self._campos, self._mensaje)
        self._19_contador(self._mm, self._campos, self._mensaje)
        self._20_mantenimiento(self._mm, self._campos, self._mensaje)
        
        self._21_canbus(self._mm, self._campos, self._mensaje)
        self._22_canbus_horas(self._mm, self._campos, self._mensaje)
        self._23_canbus_fuel(self._mm, self._campos, self._mensaje)
        self._24_canbus_extendido(self._mm, self._campos, self._mensaje)
        self._25_canbus_fms3(self._mm, self._campos, self._mensaje)

        self._26_glp_iveco_euro5(self._mm, self._campos, self._mensaje)
        self._27_knorr(self._mm, self._campos, self._mensaje)
        self._28_haldex(self._mm, self._campos, self._mensaje)
        self._29_wabco(self._mm, self._campos, self._mensaje)
        self._30_7080(self._mm, self._campos, self._mensaje)

        self._31_informacion_gsm(self._mm, self._campos, self._mensaje)
        self._32_id_movil_slave(self._mm, self._campos, self._mensaje)
        self._33_master(self._mm, self._campos, self._mensaje)
        self._34_trailer_7080(self._mm, self._campos, self._mensaje)
        self._35_ibutton(self._mm, self._campos, self._mensaje)

        self._36_palfinger(self._mm, self._campos, self._mensaje)
        self._37_ns_carrier(self._mm, self._campos, self._mensaje)
        self._38_conductor(self._mm, self._campos, self._mensaje)
        self._39_doble_conductor(self._mm, self._campos, self._mensaje)
        self._40_alarma_puerta_slave(self._mm, self._campos, self._mensaje)

        self._41_lls20160(self._mm, self._campos, self._mensaje)
        self._42_salidas_digitales(self._mm, self._campos, self._mensaje)
        self._43_bloque_almacenamiento(self._mm, self._campos, self._mensaje)
        self._44_power_supply(self._mm, self._campos, self._mensaje)
        self._45_intelliset(self._mm, self._campos, self._mensaje)
        
        self._99_finales(self._mm, self._campos, self._mensaje)
        
        return self._mensaje
        
#
#
#       
def parser_factory(context, texto):
    """Instancia el Parser en funcion del tipo de mensaje recibido
    """
    log.info("-----> Inicio")
    log.info("\t(texto): %s" % texto)
    
    parser = ParserNULL()
    campos = texto.split(",")
    
    if len(campos) < 3:
        log.warn("<----- Mensaje aparentemente incorrecto, saliendo...")
        return parser
    
    id_dev = int(campos[1])
    if not id_dev in context.get_dispositivos():
        log.warn("<----- Dispositivo %d no encontrado, saliendo..." % id_dev)
        return parser
        
    dispositivo = context.get_dispositivos()[id_dev]
    tipo = campos[2]
    
    if tipo.startswith("TDI*P"):
        parser = ParserP(context, dispositivo)
    elif tipo.startswith("TDI*GETDRIVER"):
        parser = ParserGETDRIVERINFO(context)
        
    log.info("<----- Fin")
    return parser

#
#
#
def parse(context, texto):
    """Parsea el texto y devuelve un mensaje para insertar en base de datos
    """
    log.info("-----> Inicio")
    log.info("\t(texto): %s" % texto)
    
    mensaje = parser_factory(context, texto).parse(texto);
    mensaje["fechaProceso"] = context.ahora
        
    log.info("<----- Fin")        
    return mensaje

