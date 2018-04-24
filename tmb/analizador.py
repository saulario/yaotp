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
import di
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
            td = {}
            td["sondas"] = []
            td["dispositivos"] = []
            mensaje["TEMP"] = td
        return mensaje["TEMP"]

    def _get_datos_7080(self, mensaje):
        if not "7080" in mensaje:
            mensaje["7080"] = {}
        return mensaje["7080"]
        
    def _eliminar_campos(self, cc):
        cc.pop(0)
        cc.pop(0) 
        
    def _01_identificador(self, mm, campos, mensaje):
        current = int(mm[self.IDENTIFICADOR])
        if not current:
            return
        campos.pop(0)
         
    def _02_fecha_hora(self, mm, campos, mensaje):
        current = int(mm[self.FECHA_HORA])
        if not current:
            return
        aux = ("%s %s" % (campos.pop(0), campos.pop(0)))
        mensaje["fecha"] = datetime.datetime.strptime(aux, "%d/%m/%Y %H:%M:%S")
        
    def _03_fecha_hora1(self, mm, campos, mensaje):
        current = int(mm[self.FECHA_HORA_1])
        if not current:
            return
        aux = ("%s %s" % (campos.pop(0), campos.pop(0)))
        d = self._get_datos_gps(mensaje)
        d["fecha"] = datetime.datetime.strptime(aux, "%d/%m/%Y %H:%M:%S")

    def _04_datos_gps(self, mm, campos, mensaje):
        current = int(mm[self.DATOS_GPS])
        if not current:
            return
        d = self._get_datos_gps(mensaje)
        d["posicion"] = gis.convertir_coordenada_GPS(campos.pop(0), campos.pop(0))
        d["altitud"] = round(float(campos.pop(0)))
        d["velocidad"] = round(float(campos.pop(0)))
        d["rumbo"] = int(campos.pop(0))
        d["satelites"] = int(campos.pop(0))

    def _05_pt100_internas(self, mm, campos, mensaje):
        current = int(mm[self.PT100_INTERNAS])
        if not current:
            return       
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self.PT100_INTERNAS)
        sondas = list(float(campos.pop(0)) for i in range(3 + int(mm[self.TH16])))
        for t in sondas:
            if not t is None:
                d["sondas"].append(t)
        
    def _06_pt100_externas(self, mm, campos, mensaje):
        current = int(mm[self.PT100_EXTERNAS])
        if not current:
            return
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self.PT100_EXTERNAS)
        sondas = list(float(campos.pop(0)) for i in range(3))
        for t in sondas:
            if not t is None:
                d["sondas"].append(t)        
        
    def _07_entradas_analogicas(self, mm, campos, mensaje):
        current = int(mm[self.ENTRADAS_ANALOGICAS])
        if not current:
            return
        mensaje["analogicInput"] = list(float(campos.pop(0)) for i in range(int(campos.pop(0))))
        
    def _08_transcan(self, mm, campos, mensaje):
        current = int(mm[self.TRANSCAN])
        if not current:
            return        
        d = self._get_datos_temperatura(mensaje)    
        d["dispositivos"].append(self.TRANSCAN)
        sondas = list(float(campos.pop(0)) for i in range(campos.pop(0)))
        for t in sondas:
            if not t is None:
                d["sondas"].append(t)
        
    def _09_euroscan(self, mm, campos, mensaje):
        current = int(mm[self.EUROSCAN])
        if not current:
            return        
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self.EUROSCAN)
        sondas = list(float(campos.pop(0)) for i in range(5))
        for t in sondas:
            if not t is None:
                d["sondas"].append(t)
                
    def _10_datacold(self, mm, campos, mensaje):
        current = int(mm[self.DATACOLD])
        if not current:
            return        
        d = self._get_datos_temperatura(mensaje)        
        d["dispositivos"].append(self.DATACOLD)
        sondas = list(float(campos.pop(0)) for i in range(4))
        for t in sondas:
            if not t is None:
                d["sondas"].append(t)
        d["alarmas"] = campos.pop(0)
        d["entradasDigitales"] = campos.pop(0)
        d["alarmasEEDD"] = campos.pop(0) # ignorado alarmas eedd
        
    def _fromTouchprintToCelsius(self, t):
        self._temp = float(t) / 10
        self._temp = round(((self._temp -32) / 1.8), 1)
        if self._temp < -1000:
            self._temp = None
        return self._temp

    def _11_touchprint(self, mm, campos, mensaje):
        current = int(mm[self.TOUCHPRINT])
        if not current:
            return        
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self.TOUCHPRINT)
        sondas = list(self._fromTouchprintToCelsius(campos.pop(0)) for i in range(6))
        for t in sondas:
            if not t is None:
                d["sondas"].append(t)
        
    def _12_digitales(self, mm, campos, mensaje):
        current = int(mm[self.ENTRADAS_DIGITALES_EXTENDIDAS])
        if not current:
            return   
        mensaje["entradasDigitales"] = list(campos.pop(0) for i in range(2))
             
    def _13_ibox(self, mm, campos, mensaje):
        current = int(mm[self.IBOX])
        if not current:
            return        
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self.IBOX)
        d["alarmas"] = int(campos.pop(0))
        d["tempRetorno"] = float(campos.pop(0))
        d["tempSuministro"] = float(campos.pop(0))
        d["setPoint"] = float(campos.pop(0))
        d["horasElectrico"] = float(campos.pop(0))
        d["horasMotor"] = float(campos.pop(0))
        d["horasTotales"] = float(campos.pop(0))
        d["modoOperacion"] = int(campos.pop(0))
             
    def _14_carrier(self, mm, campos, mensaje):
        current = int(mm[self.CARRIER])
        if not current:
            return        
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self.CARRIER)
        d["setPoint"] = float(campos.pop(0)) / 10
        d["modoOperacion"] = list(campos.pop(0) for i in range(2))
        d["tempRetorno"] = float(campos.pop(0)) / 10
        d["tempSuministro"] = float(campos.pop(0)) / 10
        d["presion"] = float(campos.pop(0)) / 10
        d["horasTotales"] = float(campos.pop(0))

    def _15_das(self, mm, campos, mensaje):
        current = int(mm[self.DAS])
        if not current:
            return        
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self.DAS)
        d["setPoint"] = float(campos.pop(0))
        d["tempRetorno"] = float(campos.pop(0))
        d["tempSuministro"] = float(campos.pop(0))
        
    def _16_thermo_guard_vi(self, mm, campos, mensaje):
        current = int(mm[self.THERMO_GUARD_VI])
        if not current:
            return
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self.THERMO_GUARD_VI)
        d["setPoint"] = self._fromTouchprintToCelsius(float(campos.pop(0)))
        d["tempRetorno"] = self._fromTouchprintToCelsius(float(campos.pop(0)))
        d["tempSuministro"] = self._fromTouchprintToCelsius(float(campos.pop(0)))
        
    def _17_th12online(self, mm, campos, mensaje):
        current = int(mm[self.TH12_ONLINE])
        if not current:
            return    
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self.TH12_ONLINE)
        t = float(campos.pop(0))  
        if -30 <= t <= 50:
            d["sondas"].append(self._t)
                    
    def _18_datos_gps(self, mm, campos, mensaje, esquema):
        current = int(mm[self.DATOS_GPS])
        if not current:
            return
        d = self._get_datos_gps(mensaje)
        d["kilometros"] = round(float(campos.pop(0)))
        d["entradasDigitales"] = int(campos.pop(0))
        v = di.obtener_entradas_digitales(d["entradasDigitales"], esquema)
        if not v is None:
            mensaje["DI"] = v        
        
    def _19_contador(self, mm, campos, mensaje):
        current = int(mm[self.CONTADOR])
        if not current:
            return    
        mensaje["kilometros"] = float(campos.pop(0))        

    def _20_mantenimiento(self, mm, campos, mensaje):
        current = int(mm[self.PANTALLA])
        aux = int(mm[self.LECTOR_DE_TARJETAS])
        if (not current and not aux):
            return    
        mensaje["mantenimiento"] = campos.pop(0)
        
    def _21_canbus(self, mm, campos, mensaje):
        current = int(mm[self.CANBUS])
        if not current:
            return    
        d = self._get_datos_canbus(mensaje)
        d["tacografo"] = campos.pop(0)
        v = taco.obtener_datos_tacografo(d["tacografo"])
        if not v is None:
            mensaje["TACHO"] = v            
        v = canbus.obtener_odometro(campos.pop(0))
        if not v is None:
            d["odometro"] = v            
        temperatura = campos.pop(0)
        v = canbus.obtener_temp_motor(temperatura)
        if not v is None:
            d["tempMotor"] = v
        v = canbus.obtener_temp_fuel(temperatura)
        if not v is None:
            d["tempFuel"] = v            

    def _22_canbus_horas(self, mm, campos, mensaje):
        current = int(mm[self.CANBUS_HORAS])
        if not current:
            return    
        d = self._get_datos_canbus(mensaje)
        v = canbus.obtener_horas(campos.pop(0))
        if not v is None:
            d["horas"] = v
        
    def _23_canbus_fuel(self, mm, campos, mensaje):
        current = int(mm[self.CANBUS_FUEL])
        if not current:
            return    
        d = self._get_datos_canbus(mensaje)
        v = canbus.obtener_combustible(campos.pop(0))
        if not v is None:
            d["combustible"] = v

    def _24_canbus_extendido(self, mm, campos, mensaje):
        current = int(mm[self.CANBUS_EXTENDIDO])
        if not current:
            return    
        d = self._get_datos_canbus(mensaje)
        d["ccvs"] = campos.pop(0)
        v = canbus.obtener_velocidad(d["ccvs"])
        if not v is None:
            d["velocidad"] = v
        d["eec2"] = campos.pop(0)
        d["eec1"] = campos.pop(0)
        d["fecha"] = campos.pop(0)
        d["display"] = campos.pop(0)
        d["peso"] = campos.pop(0)

    def _25_canbus_fms3(self, mm, campos, mensaje):
        current = int(mm[self.CANBUS_FMS3])
        if not current:
            return    
        d = self._get_datos_canbus(mensaje)
        d["lfe"] = campos.pop(0)
        d["erc1"] = campos.pop(0)
        d["dc1"] = campos.pop(0)
        d["dc2"] = campos.pop(0)
        d["etc2"] = campos.pop(0)
        d["asc4"] = campos.pop(0)
                
    def _26_glp_iveco_euro5(self, mm, campos, mensaje):
        current = int(mm[self.GLP_IVECO_EURO5])
        if not current:
            return
        d = {}
        d["mp3msg1"] = campos.pop(0)
        d["mp3msg2"] = campos.pop(0)
        mensaje["IVECO"] = d
            
    def _27_knorr(self, mm, campos, mensaje):
        current = int(mm[self.KNORR])
        if not current:
            return
        d = {}
        d["hrdv"] = campos.pop(0)
        d["speed"] = campos.pop(0)
        d["weight"] = campos.pop(0)
        d["brakes"] = campos.pop(0)
        d["ebsprop1"] = campos.pop(0)
        d["ebsprop4"] = campos.pop(0)
        d["ebsprop5"] = campos.pop(0)
        d["ebsprop6"] = campos.pop(0)
        d["ebsprop7"] = campos.pop(0)
        d["ebsprop8"] = campos.pop(0)
        d["ebsprop9"] = campos.pop(0)                                        
        mensaje["KNORR"] = d
            
    def _28_haldex(self, mm, campos, mensaje):
        current = int(mm[self.HALDEX])
        if not current:
            return
        d = {}
        d["speed"] = campos.pop(0)
        d["pressures"] = campos.pop(0)
        d["odometer"] = campos.pop(0)
        d["3m"] = campos.pop(0)
        mensaje["HALDEX"] = d
            
    def _29_wabco(self, mm, campos, mensaje):
        current = int(mm[self.WABCO])
        if not current:
            return
        d = {}
        d["ebs11"] = campos.pop(0)
        d["ebs12"] = campos.pop(0)
        d["ebs21"] = campos.pop(0)
        d["ebs22"] = campos.pop(0)
        d["ebs23"] = campos.pop(0)
        d["ebs24"] = campos.pop(0)
        d["ebs25"] = campos.pop(0)
        d["rge11"] = campos.pop(0)
        d["rge21"] = campos.pop(0)
        d["rge22"] = campos.pop(0)
        d["rge23"] = campos.pop(0)
        d["hrvd"] = campos.pop(0)
        mensaje["WABCO"] = d;
            
    def _30_7080(self, mm, campos, mensaje):
        current = int(mm[self.DL_7080])
        if not current:
            return
        d = self._get_datos_7080(mensaje)    
        d["kilometros"] = float(campos.pop(0))
        d["velocidad"] = float(campos.pop(0))
        d["rpm"] = float(campos.pop(0))
        
    def _31_informacion_gsm(self, mm, campos, mensaje):
        current = int(mm[self.INFORMACION_GSM])
        if not current:
            return
        d = {}    
        current = int(mm[self.SOCKET])
        if not current:
            d["operador"] = campos.pop(0)
        d["calidadSenal"] = campos.pop(0)
        mensaje["GSM"] = d
                
    def _32_id_movil_slave(self, mm, campos, mensaje):
        pass
    
    def _33_master(self, mm, campos, mensaje):
        current = int(mm[self.MASTER])
        if not current:
            return
        raise RuntimeError("33_master no implementado")
                    
    def _34_trailer_7080(self, mm, campos, mensaje):
        current = int(mm[self.DL_7080_TRAILER])
        if not current:
            return            
        d = self._get_datos_7080(mensaje)
        d["idEsclavo"] = campos.pop(0)
        
    def _35_ibutton(self, mm, campos, mensaje):
        current = int(mm[self.IBUTTON])
        if not current:
            return            
        mensaje["IBUTTON"] = campos.pop(0)
        
    def _36_palfinger(self, mm, campos, mensaje):
        current = int(mm[self.PALFINGER])
        if not current:
            return
        mensaje["PALFINGER"] = campos.pop(0)
                    
    def _37_ns_carrier(self, mm, campos, mensaje):
        current = int(mm[self.NS_CARRIER])
        if not current:
            return
        d = self._get_datos_temperatura(mensaje)
        d["numeroSerie"] = campos.pop(0)
                    
    def _38_conductor(self, mm, campos, mensaje):
        current = int(mm[self.CONDUCTOR])
        if not current:
            return
        d = self._get_datos_conductores(mensaje)
        d["id1"] = campos.pop(0)
                    
    def _39_doble_conductor(self, mm, campos, mensaje):
        current = int(mm[self.DOBLE_CONDUCTOR])
        if not current:
            return
        d = self._get_datos_conductores(mensaje)
        d["id1"] = campos.pop(0)        
        d["id2"] = campos.pop(0)
                    
    def _40_alarma_puerta_slave(self, mm, campos, mensaje):
        current = int(mm[self.ALARMA_PUERTA_SLAVE])
        if not current:
            return
        mensaje["alarmaPuertaSlave"] = campos.pop(0)

    def _41_lls20160(self, mm, campos, mensaje):
        current = int(mm[self.LLS20160])
        if not current:
            return
        d = {}
        d["tempSonda1"] = float(campos.pop(0))
        d["tempSonda2"] = float(campos.pop(0))
        d["fuelSonda1"] = float(campos.pop(0))
        d["fuelSonda1"] = float(campos.pop(0))
        mensaje["LLS20160"] = d
                    
    def _42_salidas_digitales(self, mm, campos, mensaje):
        current = int(mm[self.SALIDAS_DIGITALES])
        if not current:
            return
        mensaje["salidasDigitales"] = campos.pop(0)
                    
    def _43_bloque_almacenamiento(self, mm, campos, mensaje):
        current = int(mm[self.BLOQUE_ALMACENAMIENTO])
        if not current:
            return
        mensaje["salidasDigitales"] = campos.pop(0)
                    
    def _44_power_supply(self, mm, campos, mensaje):
        current = int(mm[self.POWER_SUPPLY])
        if not current:
            return
        mensaje["alimentacion"] = float(campos.pop(0))
                    
    def _45_intelliset(self, mm, campos, mensaje):
        current = int(mm[self.INTELLISET])
        if not current:
            return            
        d = {}
        d["installed"] = campos.pop(0)
        d["loadedNumber"] = campos.pop(0)
        d["validForModel"] = campos.pop(0)
        d["state"] = campos.pop(0)
        d["name"] = campos.pop(0)
        mensaje["INTELLISET"] = d
        
    def _99_finales(self, mm, campos, mensaje):        
        mensaje["segmento"] = campos.pop(0)
        mensaje["offset"] = campos.pop(0)

    def parse(self, texto):
        campos = texto.split(",")
        mensaje = {}
        mensaje["idDispositivo"] = self._dispositivo["ID_MOVIL"]
        mensaje["matricula"] = self._dispositivo["REGISTRATION"]
        mensaje["tipoMensaje"] = "TDI*P"
        
        if self._context.debug == mensaje["idDispositivo"]:
            mensaje["dbgMascara"] = int(self._dispositivo["MASK"], base=2)
            mensaje["dbgMensaje"] = texto
        
        mm = self._mascara       
        self._eliminar_campos(campos)
        
        self._01_identificador(mm, campos, mensaje)
        self._02_fecha_hora(mm, campos, mensaje)
        self._03_fecha_hora1(mm, campos, mensaje)
        self._04_datos_gps(mm, campos, mensaje)
        self._05_pt100_internas(mm, campos, mensaje) 
        
        self._06_pt100_externas(mm, campos, mensaje)
        self._07_entradas_analogicas(mm, campos, mensaje)
        self._08_transcan(mm, campos, mensaje)
        self._09_euroscan(mm, campos, mensaje)
        self._10_datacold(mm, campos, mensaje)
        
        self._11_touchprint(mm, campos, mensaje)
        self._12_digitales(mm, campos, mensaje)
        self._13_ibox(mm, campos, mensaje)
        self._14_carrier(mm, campos, mensaje)
        self._15_das(mm, campos, mensaje)
        
        self._16_thermo_guard_vi(mm, campos, mensaje)
        self._17_th12online(mm, campos, mensaje)
        self._18_datos_gps(mm, campos, mensaje,
                           self._dispositivo["SCHEMATYPE"])
        self._19_contador(mm, campos, mensaje)
        self._20_mantenimiento(mm, campos, mensaje)
        
        self._21_canbus(mm, campos, mensaje)
        self._22_canbus_horas(mm, campos, mensaje)
        self._23_canbus_fuel(mm, campos, mensaje)
        self._24_canbus_extendido(mm, campos, mensaje)
        self._25_canbus_fms3(mm, campos, mensaje)

        self._26_glp_iveco_euro5(mm, campos, mensaje)
        self._27_knorr(mm, campos, mensaje)
        self._28_haldex(mm, campos, mensaje)
        self._29_wabco(mm, campos, mensaje)
        self._30_7080(mm, campos, mensaje)

        self._31_informacion_gsm(mm, campos, mensaje)
        self._32_id_movil_slave(mm, campos, mensaje)
        self._33_master(mm, campos, mensaje)
        self._34_trailer_7080(mm, campos, mensaje)
        self._35_ibutton(mm, campos, mensaje)

        self._36_palfinger(mm, campos, mensaje)
        self._37_ns_carrier(mm, campos, mensaje)
        self._38_conductor(mm, campos, mensaje)
        self._39_doble_conductor(mm, campos, mensaje)
        self._40_alarma_puerta_slave(mm, campos, mensaje)

        self._41_lls20160(mm, campos, mensaje)
        self._42_salidas_digitales(mm, campos, mensaje)
        self._43_bloque_almacenamiento(mm, campos, mensaje)
        self._44_power_supply(mm, campos, mensaje)
        self._45_intelliset(mm, campos, mensaje)
        
        self._99_finales(mm, campos, mensaje)
        
        return mensaje
        
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

#if __name__ == "__main__":
#
#    from context import Context
#    c = Context()
#    d = {
#            "ID_MOVIL": 21142,
#            "MASK": "11110000001000101001000000000000000000000100111000",
#            "SCHEMATYPE": "Remolque_Frigorifico",
#            "REGISTRATION": "R1781BCW"
#            }
#    c._dispositivos = dict()
#    c._dispositivos[21142] = d
#
#    
#    m = "GPRS/SOCKET,21142,TDI*P=21142,22/04/2018,22:08:27,22/04/2018,22:08:27,041:46:14.5972N,001:13:05.8199W,262,0,113,10,639,628,-32768,-32768,-32768,695,0,0,0,39607.0,49,0,00000000000000,000010a100008a,00d39245001000,f,21403,9,11.77,00bc,aee4,CRCd26d"
#    parse(c, m)