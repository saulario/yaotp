#!/usr/bin/python3
# -*- coding: utf-8 -*-

import datetime
import logging

import dbmparser.canbus as canbus
import dbmparser.di as di
import dbmparser.ebs as ebs
import dbmparser.gis as gis
import dbmparser.safe as safe
import dbmparser.tacografo as taco

log = logging.getLogger(__name__)

class ParserP():
    
    IDENTIFICADOR                   = 1
    FECHA_HORA                      = 1 << 1
    FECHA_HORA_1                    = 1 << 2
    DATOS_GPS                       = 1 << 3
    PT100_EXTERNAS                  = 1 << 4
    ENTRADAS_ANALOGICAS             = 1 << 5
    ENTRADAS_DIGITALES_EXTENDIDAS   = 1 << 6
    IBOX                            = 1 << 7
    CARRIER                         = 1 << 8
    CONTADOR                        = 1 << 9        # 10
    PANTALLA                        = 1 << 10
    CONDUCTOR                       = 1 << 11
    CANBUS                          = 1 << 12
    DL_7080                         = 1 << 13
    INFORMACION_GSM                 = 1 << 14
    PALFINGER                       = 1 << 15
    SOCKET                          = 1 << 16
    MASTER                          = 1 << 17
    NS_CARRIER                      = 1 << 18
    DAS                             = 1 << 19       # 20
    TRANSCAN                        = 1 << 20
    FRIO_ESCLAVO                    = 1 << 21
    CANBUS_FUEL                     = 1 << 22
    BLOQUE_ALMACENAMIENTO           = 1 << 23
    TH12_ONLINE                     = 1 << 24
    LECTOR_DE_TARJETAS              = 1 << 25
    CANBUS_HORAS                    = 1 << 26
    DOBLE_CONDUCTOR                 = 1 << 27
    MULTITEMPERATURA                = 1 << 28
    MULTITEMP_AMPLIADA              = 1 << 29        # 30
    ALARMA_PUERTA_SLAVE             = 1 << 30
    EUROSCAN                        = 1 << 31
    DATACOLD                        = 1 << 32
    CANBUS_EXTENDIDO                = 1 << 33
    THERMO_GUARD_VI                 = 1 << 34
    DL_7080_TRAILER                 = 1 << 35
    SALIDAS_DIGITALES               = 1 << 36
    IBUTTON                         = 1 << 37
    PT100_INTERNAS                  = 1 << 38
    KNORR                           = 1 << 39       # 40
    WABCO                           = 1 << 40
    HALDEX                          = 1 << 41
    GLP_IVECO_EURO5                 = 1 << 42
    CANBUS_FMS3                     = 1 << 43
    TOUCHPRINT                      = 1 << 44
    POWER_SUPPLY                    = 1 << 45
    CRC                             = 1 << 46
    LLS20160                        = 1 << 47
    INTELLISET                      = 1 << 48
    TH16                            = 1 << 49       # 50
    HWASUNG_THERMO                  = 1 << 50
    APACHE                          = 1 << 51
    CARRIER_PARTNER                 = 1 << 52
    CARRIER_3RD_PARTY               = 1 << 53
    TRANSCAN_ADVANCE                = 1 << 54

    def __init__(self, context, dispositivo):
        self._context = context
        self._dispositivo = dispositivo 
        self._mascara = dispositivo["maskBin"]

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
    
    def _get_bit(self, v):
        return len(bin(v)[2:])
        
    def _eliminar_campos(self, cc):
        cc.pop(0)
        cc.pop(0) 
        
    def _01_identificador(self, campos, mensaje):
        if not self._mascara & self.IDENTIFICADOR:
            return
        campos.pop(0)
         
    def _02_fecha_hora(self, campos, mensaje):
        if not self._mascara & self.FECHA_HORA:
            return
        log.debug("+")
        aux = ("%s %s" % (campos.pop(0), campos.pop(0)))
        mensaje["fecha"] = safe.to_datetime(aux, "%d/%m/%Y %H:%M:%S")
        
    def _03_fecha_hora1(self, campos, mensaje):
        if not self._mascara & self.FECHA_HORA_1:
            return
        log.debug("+")            
        aux = ("%s %s" % (campos.pop(0), campos.pop(0)))
        d = self._get_datos_gps(mensaje)
        d["fecha"] = safe.to_datetime(aux, "%d/%m/%Y %H:%M:%S")

    def _04_datos_gps(self, campos, mensaje):
        if not self._mascara & self.DATOS_GPS:
            return
        log.debug("+")
        d = self._get_datos_gps(mensaje)
        d["posicion"] = gis.convertir_coordenada_GPS(campos.pop(0), 
                 campos.pop(0))
        d["altitud"] = round(float(campos.pop(0)))
        d["velocidad"] = round(float(campos.pop(0)))
        d["rumbo"] = int(campos.pop(0))
        d["satelites"] = int(campos.pop(0))

    def _05_pt100_internas(self, campos, mensaje):
        if not self._mascara & self.PT100_INTERNAS:
            return    
        log.debug("+")   
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self._get_bit(self.PT100_INTERNAS))
        i = int(self._mascara & self.TH16)
        sondas = list(safe.to_float(campos.pop(0)) for i in range(3 + i))
        for t in sondas:
            if not t is None:
                d["sondas"].append(t)
        
    def _06_pt100_externas(self, campos, mensaje):
        if not self._mascara & self.PT100_EXTERNAS:
            return
        log.debug("+")
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self._get_bit(self.PT100_EXTERNAS))
        sondas = list(float(campos.pop(0)) for i in range(3))
        for t in sondas:
            if not t is None:
                d["sondas"].append(t)        
        
    def _07_entradas_analogicas(self, campos, mensaje):
        if not self._mascara & self.ENTRADAS_ANALOGICAS:
            return
        log.debug("+")
        mensaje["analogicInput"] = list(float(campos.pop(0)) 
                for i in range(int(campos.pop(0))))

    def _08_apache(self, campos, mensaje):
        if not self._mascara & self.APACHE:
            return
        log.debug("+")
        d = self._get_datos_temperatura(mensaje)    
        d["dispositivos"].append(self._get_bit(self.APACHE))
        d["sondas"] = list(float(campos.pop(0)) for i in range(2))

    def _09_transcan(self, campos, mensaje):
        if not self._mascara & self.TRANSCAN:
            return        
        log.debug("+")
        d = self._get_datos_temperatura(mensaje)    
        d["dispositivos"].append(self._get_bit(self.TRANSCAN))
        sondas = list(float(campos.pop(0)) for i in range(int(self._dispositivo["TRASCANCHANNELS"])))
        for t in sondas:
            if not t is None:
                d["sondas"].append(t)

    def _10_transcan_advance(self, campos, mensaje):
        if not self._mascara & self.TRANSCAN_ADVANCE:
            return
        log.debug("+")
        d = self._get_datos_temperatura(mensaje)    
        d["dispositivos"].append(self._get_bit(self.TRANSCAN_ADVANCE))
        d["sondas"] = list(float(campos.pop(0)) for i in range(8))
        d1 = {}
        d1["humedad"] = campos.pop(0)
        d1["entradas"] = campos.pop(0)
        d1["alarmas"] = campos.pop(0)
        d["TRANSCAN_ADVANCE"] = d1

    def _11_euroscan(self, campos, mensaje):
        if not self._mascara & self.EUROSCAN:
            return        
        log.debug("+")
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self._get_bit(self.EUROSCAN))
        sondas = list(float(campos.pop(0)) for i in range(5))
        for t in sondas:
            if not t is None:
                d["sondas"].append(t)
                
    def _12_datacold(self, campos, mensaje):
        if not self._mascara & self.DATACOLD:
            return    
        log.debug("+")    
        d = self._get_datos_temperatura(mensaje)        
        d["dispositivos"].append(self._get_bit(self.DATACOLD))
        sondas = list(float(campos.pop(0)) for i in range(4))
        for t in sondas:
            if not t is None:
                d["sondas"].append(t)
        d1 = {}
        d1["alarmas"] = campos.pop(0)
        d1["entradasDigitales"] = campos.pop(0)
        d1["alarmasEEDD"] = campos.pop(0) 
        d["DATACOLD"] = d1
        
    def _fromTouchprintToCelsius(self, t):
        temp = float(t)
        if temp is None:
            return None
        temp = temp / 10
        temp = round(((temp -32) / 1.8), 1)
        if temp < -1000:
            temp = None
        return temp

    def _13_touchprint(self, campos, mensaje):
        if not self._mascara & self.TOUCHPRINT:
            return    
        log.debug("+")    
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self._get_bit(self.TOUCHPRINT))
        sondas = list(self._fromTouchprintToCelsius(campos.pop(0)) 
                for i in range(6))
        for t in sondas:
            if not t is None:
                d["sondas"].append(t)
        
    def _14_digitales(self, campos, mensaje):
        if not self._mascara & self.ENTRADAS_DIGITALES_EXTENDIDAS:
            return   
        log.debug("+")
        mensaje["entradasDigitalesExt"] = list(campos.pop(0) for i in range(2))
             
    def _15_ibox(self, campos, mensaje):
        if not self._mascara & self.IBOX:
            return    
        log.debug("+")    
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self._get_bit(self.IBOX))        
        d1 = {}
        d1["alarmas"] = int(campos.pop(0))
        d["tempRetorno"] = float(campos.pop(0))
        d["tempSuministro"] = float(campos.pop(0))
        d["setPoint"] = float(campos.pop(0))
        d1["horasElectrico"] = float(campos.pop(0))
        d1["horasMotor"] = float(campos.pop(0))
        d1["horasTotales"] = float(campos.pop(0))
        d1["modoOperacion"] = int(campos.pop(0))
        d["IBOX"] = d1

    def _16_hwasung_termo(self, campos, mensaje):
        if not self._mascara & self.HWASUNG_THERMO:
            return
        log.debug("+")
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self._get_bit(self.HWASUNG_THERMO))        
        d1 = {}                 
        d["zonas"] = []
        z0 = {}
        z1 = {}
        d["zonas"].append(z0)
        d["zonas"].append(z1)
        z0["setPoint"] = float(campos.pop(0))
        z0["tempRetorno"] = float(campos.pop(0))
        z1["setPoint"] = float(campos.pop(0))
        z1["tempRetorno"] = float(campos.pop(0))
        d1["horasTotales"] = float(campos.pop(0))
        d1["voltaje"] = float(campos.pop(0))
        d1["motorEncendido"] = float(campos.pop(0))
        d["HWASUNG_THERMO"] = d1
             
    def _17_carrier(self, campos, mensaje):
        if not self._mascara & self.CARRIER:
            return    
        log.debug("+")    
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self._get_bit(self.CARRIER))
        d1 = {}
        d["setPoint"] = safe.divide(safe.to_float(campos.pop(0)), 10)
        d1["modoOperacion"] = list(campos.pop(0) for i in range(2))
        d["tempRetorno"] = float(campos.pop(0)) / 10
        d["tempSuministro"] = safe.divide(safe.to_float(campos.pop(0)), 10)
        d1["presion"] = float(campos.pop(0)) / 10
        d1["horasTotales"] = float(campos.pop(0))
        d["CARRIER"] = d1

    def _18_carrier_3rd_party(self, campos, mensaje):
        if not self._mascara & self.CARRIER_3RD_PARTY:
            return
        log.debug("+")
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self._get_bit(self.CARRIER_3RD_PARTY))
        d1 = {}                 
        d["zonas"] = []
        z0 = {}
        z1 = {}
        z2 = {}
        d["zonas"].append(z0)
        d["zonas"].append(z1)
        d["zonas"].append(z2)
        z0["setPoint"] = float(campos.pop(0)) / 32
        z1["setPoint"] = float(campos.pop(0)) / 32
        z2["setPoint"] = float(campos.pop(0)) / 32
        d1["pf0"] = campos.pop(0)
        d1["pf1"] = campos.pop(0)
        z0["tempRetorno"] = float(campos.pop(0)) / 32
        z1["tempRetorno"] = float(campos.pop(0)) / 32
        z2["tempRetorno"] = float(campos.pop(0)) / 32                
        z0["tempSuministro"] = float(campos.pop(0)) / 32
        z1["tempSuministro"] = float(campos.pop(0)) / 32
        z2["tempSuministro"] = float(campos.pop(0)) / 32
        d1["presion"] = float(campos.pop(0)) / 32
        d1["horasTotales"] = float(campos.pop(0))
        d["CARRIER_3RD_PARTY"] = d1

    def _19_carrier_partner(self, campos, mensaje):
        if not self._mascara & self.CARRIER_PARTNER:
            return
        log.debug("+")
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self._get_bit(self.CARRIER_PARTNER))
        d["sondas"] = list(campos.pop(0) for i in range(4))
        d["zonas"] = []
        z0 = {}
        z1 = {}
        z2 = {}
        d["zonas"].append(z0)
        d["zonas"].append(z1)
        d["zonas"].append(z2)
        d1 = {}                 
        d1["digitalSensor"] = campos.pop(0)
        d1["FridgeCompartmentsAvailability"] = campos.pop(0)
        d1["FridgeRunMode"] = int(campos.pop(0))
        d1["FridgePowerMode"] = int(campos.pop(0))
        d1["FridgeSpeedMode"] = int(campos.pop(0))
        d1["FridgeBattery"] = {}
        aux = campos.pop(0).split(";")
        if len(aux) == 2:
            d1["FridgeBattery"]["state"] = aux[0]
            d1["FridgeBattery"]["voltage"] = float(aux[1])
        d1["FridgeFuel"] = {}
        aux = campos.pop(0).split(";")
        if len(aux) == 2:        
            d1["FridgeFuel"]["state"] = aux[0]
            d1["FridgeFuel"]["level"] = float(aux[1])
        d1["FridgeAmbientTemperature"] = float(campos.pop(0))

        # en cada zona simplifico poniendo setpoint y retorno de la sonda 1
        # aunque parece que aquí habría que hacer un zonas genérico y un zonas de fabricante
        z0["FridgeCompartmentState"] = int(campos.pop(0))
        z0["FridgeCompartmentMode"] = int(campos.pop(0))
        z0["setPoint"] = float(campos.pop(0))
        z0["FridgeCompartmentSupplyAirSensor"] = {}
        aux = campos.pop(0).split(";")
        if len(aux) == 2:        
            d1["FridgeCompartmentSupplyAirSensor"]["sensor1"] = float(aux[0])
            z0["tempSuministro"] = float(aux[0])
            d1["FridgeCompartmentSupplyAirSensor"]["sensor2"] = float(aux[1])
        z0["FridgeCompartmentReturnAirSensor"] = {}
        aux = campos.pop(0).split(";")
        if len(aux) == 2:        
            d1["FridgeCompartmentReturnAirSensor"]["sensor1"] = float(aux[0])
            z0["tempRetorno"] = float(aux[0])
            d1["FridgeCompartmentReturnAirSensor"]["sensor2"] = float(aux[1])
        z0["FridgeCompartmentEvaporatorTemperature"] = float(campos.pop(0))

        z1["FridgeCompartmentState"] = int(campos.pop(0))
        z1["FridgeCompartmentMode"] = int(campos.pop(0))
        z1["setPoint"] = float(campos.pop(0))
        z1["FridgeCompartmentSupplyAirSensor"] = {}
        aux = campos.pop(0).split(";")
        if len(aux) == 2:        
            d1["FridgeCompartmentSupplyAirSensor"]["sensor1"] = float(aux[0])
            z1["tempSuministro"] = float(aux[0])
            d1["FridgeCompartmentSupplyAirSensor"]["sensor2"] = float(aux[1])
        z1["FridgeCompartmentReturnAirSensor"] = {}
        aux = campos.pop(0).split(";")
        if len(aux) == 2:        
            d1["FridgeCompartmentReturnAirSensor"]["sensor1"] = float(aux[0])
            z1["tempRetorno"] = float(aux[0])
            d1["FridgeCompartmentReturnAirSensor"]["sensor2"] = float(aux[1])
        z1["FridgeCompartmentEvaporatorTemperature"] = float(campos.pop(0))

        z2["FridgeCompartmentState"] = int(campos.pop(0))
        z2["FridgeCompartmentMode"] = int(campos.pop(0))
        z2["setPoint"] = float(campos.pop(0))
        z2["FridgeCompartmentSupplyAirSensor"] = {}
        aux = campos.pop(0).split(";")
        if len(aux) == 2:        
            d1["FridgeCompartmentSupplyAirSensor"]["sensor1"] = float(aux[0])
            z2["tempSuministro"] = float(aux[0])
            d1["FridgeCompartmentSupplyAirSensor"]["sensor2"] = float(aux[1])
        z2["FridgeCompartmentReturnAirSensor"] = {}
        aux = campos.pop(0).split(";")
        if len(aux) == 2:        
            d1["FridgeCompartmentReturnAirSensor"]["sensor1"] = float(aux[0])
            z2["tempRetorno"] = float(aux[0])
            d1["FridgeCompartmentReturnAirSensor"]["sensor2"] = float(aux[1])
        z2["FridgeCompartmentEvaporatorTemperature"] = float(campos.pop(0))

        d1["FridgeHoursElectric"] = float(campos.pop(0))
        d1["FridgeHoursStandby"] = float(campos.pop(0))
        d1["FridgeHoursDiesel"] = float(campos.pop(0))
        d1["horasTotales"] = d1["FridgeHoursElectric"] + d1["FridgeHoursStandby"] + d1["FridgeHoursDiesel"]

        d["CARRIER_PARTNER"] = d1

    def _20_das(self, campos, mensaje):
        if not self._mascara & self.DAS:
            return    
        log.debug("+")    
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self._get_bit(self.DAS))
        d["setPoint"] = float(campos.pop(0))
        d["tempRetorno"] = float(campos.pop(0))
        d["tempSuministro"] = float(campos.pop(0))
        
    def _21_thermo_guard_vi(self, campos, mensaje):
        if not self._mascara & self.THERMO_GUARD_VI:
            return
        log.debug("+")
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self._get_bit(self.THERMO_GUARD_VI))
        d["setPoint"] = self._fromTouchprintToCelsius(float(campos.pop(0)))
        d["tempRetorno"] = self._fromTouchprintToCelsius(float(campos.pop(0)))
        d["tempSuministro"] = self._fromTouchprintToCelsius(
                float(campos.pop(0)))
        
    def _22_th12online(self, campos, mensaje):
        if not self._mascara & self.TH12_ONLINE:
            return    
        log.debug("+")
        d = self._get_datos_temperatura(mensaje)
        d["dispositivos"].append(self._get_bit(self.TH12_ONLINE))
        t = float(campos.pop(0))  
        if -30 <= t <= 50:
            d["sondas"].append(t)
                    
    def _23_datos_gps(self, campos, mensaje):
        if not self._mascara & self.DATOS_GPS:
            return
        log.debug("+")
        d = self._get_datos_gps(mensaje)
        d["kilometros"] = round(float(campos.pop(0)))
        d["entradasDigitales"] = campos.pop(0)
        v = di.obtener_entradas_digitales(d["entradasDigitales"], 
                                          self._dispositivo["SCHEMATYPE"],
                                          self._mascara & self.TH16)
        if not v is None:
            mensaje["DI"] = v        
        
    def _24_contador(self, campos, mensaje):
        if not self._mascara & self.CONTADOR:
            return    
        log.debug("+")
        mensaje["kilometros"] = float(campos.pop(0))        

    def _25_mantenimiento(self, campos, mensaje):
        current = self._mascara & self.PANTALLA
        aux = self._mascara & self.LECTOR_DE_TARJETAS        
        if not current and not aux:
            return    
        mensaje["mantenimiento"] = campos.pop(0)
        
    def _26_canbus(self, campos, mensaje):
        if not self._mascara & self.CANBUS:
            return    
        log.debug("+")
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

    def _27_canbus_horas(self, campos, mensaje):
        if not self._mascara & self.CANBUS_HORAS:
            return    
        log.debug("+")
        d = self._get_datos_canbus(mensaje)
        v = canbus.obtener_horas(campos.pop(0))
        if not v is None:
            d["horas"] = v
        
    def _28_canbus_fuel(self, campos, mensaje):
        if not self._mascara & self.CANBUS_FUEL:
            return    
        log.debug("+")
        d = self._get_datos_canbus(mensaje)
        v = canbus.obtener_combustible(campos.pop(0))
        if not v is None:
            d["combustible"] = v

    def _29_canbus_extendido(self, campos, mensaje):
        if not self._mascara & self.CANBUS_EXTENDIDO:
            return    
        log.debug("+")
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

    def _30_canbus_fms3(self, campos, mensaje):
        if not self._mascara & self.CANBUS_FMS3:
            return    
        log.debug("+")
        d = self._get_datos_canbus(mensaje)
        d["lfe"] = campos.pop(0)
        d["erc1"] = campos.pop(0)
        d["dc1"] = campos.pop(0)
        d["dc2"] = campos.pop(0)
        d["etc2"] = campos.pop(0)
        d["asc4"] = campos.pop(0)
                
    def _31_glp_iveco_euro5(self, campos, mensaje):
        if not self._mascara & self.GLP_IVECO_EURO5:
            return
        log.debug("+")
        d = {}
        d["mp3msg1"] = campos.pop(0)
        d["mp3msg2"] = campos.pop(0)
        mensaje["IVECO"] = d
            
    def _32_knorr(self, campos, mensaje):
        if not self._mascara & self.KNORR:
            return
        log.debug("+")
        d = {}
        d["hrvd"] = campos.pop(0)
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
        mensaje["EBS"] = ebs.obtener_ebs_knorr(d)
            
    def _33_haldex(self, campos, mensaje):
        if not self._mascara & self.HALDEX:
            return
        log.debug("+")
        d = {}
        d["speed"] = campos.pop(0)
        d["pressures"] = campos.pop(0)
        d["odometer"] = campos.pop(0)
        d["3m"] = campos.pop(0)
        mensaje["EBS"] = ebs.obtener_ebs_haldex(d)       
            
    def _34_wabco(self, campos, mensaje):
        if not self._mascara & self.WABCO:
            return
        log.debug("+")
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
        d["hrdv"] = campos.pop(0)
        mensaje["EBS"] = ebs.obtener_ebs_wabco(d)
            
    def _35_7080(self, campos, mensaje):
        if not self._mascara & self.DL_7080:
            return
        log.debug("+")
        d = self._get_datos_7080(mensaje)    
        d["kilometros"] = float(campos.pop(0))
        d["velocidad"] = float(campos.pop(0))
        d["rpm"] = float(campos.pop(0))
        
    def _36_informacion_gsm(self, campos, mensaje):
        if not self._mascara & self.INFORMACION_GSM:
            return
        log.debug("+")
        d = {}    
        if self._mascara & self.SOCKET:
            d["operador"] = campos.pop(0)       # revisar
        d["calidadSenal"] = safe.to_int(campos.pop(0))
        mensaje["GSM"] = d
                
    def _37_id_movil_slave(self, campos, mensaje):
        pass
    
    def _38_master(self, campos, mensaje):
        if not self._mascara & self.MASTER:
            return
        log.debug("+")
        raise RuntimeError("38_master no implementado")
                    
    def _39_trailer_7080(self, campos, mensaje):
        if not self._mascara & self.DL_7080_TRAILER:
            return    
        log.debug("+")        
        d = self._get_datos_7080(mensaje)
        d["idEsclavo"] = campos.pop(0)
        
    def _40_ibutton(self, campos, mensaje):
        if not self._mascara & self.IBUTTON:
            return    
        log.debug("+")        
        mensaje["IBUTTON"] = campos.pop(0)
        
    def _41_palfinger(self, campos, mensaje):
        if not self._mascara & self.PALFINGER:
            return
        log.debug("+")
        mensaje["PALFINGER"] = campos.pop(0)
                    
    def _42_ns_carrier(self, campos, mensaje):
        if not self._mascara & self.NS_CARRIER:
            return
        log.debug("+")
        d = self._get_datos_temperatura(mensaje)
        if not "CARRIER" in d:
            d["CARRIER"] = {}        
        d1 = d["CARRIER"]
        d1["numeroSerie"] = campos.pop(0)
                    
    def _43_conductor(self, campos, mensaje):
        if not self._mascara & self.CONDUCTOR:
            return
        log.debug("+")
        d = self._get_datos_conductores(mensaje)
        d["id1"] = campos.pop(0)
                    
    def _44_doble_conductor(self, campos, mensaje):
        if not self._mascara & self.DOBLE_CONDUCTOR:
            return
        log.debug("+")
        d = self._get_datos_conductores(mensaje)
        d["id1"] = campos.pop(0)        
        d["id2"] = campos.pop(0)
                    
    def _45_alarma_puerta_slave(self, campos, mensaje):
        if not self._mascara & self.ALARMA_PUERTA_SLAVE:
            return
        log.debug("+")
        mensaje["alarmaPuertaSlave"] = campos.pop(0)

    def _46_lls20160(self, campos, mensaje):
        if not self._mascara & self.LLS20160:
            return
        log.debug("+")
        d = {}
        d["tempSonda1"] = float(campos.pop(0))
        d["tempSonda2"] = float(campos.pop(0))
        d["fuelSonda1"] = float(campos.pop(0))
        d["fuelSonda1"] = float(campos.pop(0))
        mensaje["LLS20160"] = d
                    
    def _47_salidas_digitales(self, campos, mensaje):
        if not self._mascara & self.SALIDAS_DIGITALES:
            return
        log.debug("+")
        mensaje["salidasDigitales"] = campos.pop(0)
                    
    def _48_bloque_almacenamiento(self, campos, mensaje):
        if not self._mascara & self.BLOQUE_ALMACENAMIENTO:
            return
        log.debug("+")
        mensaje["salidasDigitales"] = campos.pop(0)
                    
    def _49_power_supply(self, campos, mensaje):
        if not self._mascara & self.POWER_SUPPLY:
            return
        log.debug("+")
        try:
            mensaje["alimentacion"] = float(campos.pop(0))
        except ValueError:
            mensaje["alimentacion"] = float("nan")
                    
    def _50_intelliset(self, campos, mensaje):
        if not self._mascara & self.INTELLISET:
            return    
        log.debug("+")        
        d = {}
        d["installed"] = campos.pop(0)
        d["loadedNumber"] = campos.pop(0)
        d["validForModel"] = campos.pop(0)
        d["state"] = campos.pop(0)
        d["name"] = campos.pop(0)
        mensaje["INTELLISET"] = d
        
    def _99_finales(self, campos, mensaje):        
        log.debug("+")
        mensaje["segmento"] = campos.pop(0)
        mensaje["offset"] = campos.pop(0)

    def parse(self, texto):
        log.debug("\t%s" % texto)
        campos = texto.split(",")
        mensaje = {}
        mensaje["idDispositivo"] = self._dispositivo["ID_MOVIL"]
        mensaje["matricula"] = self._dispositivo["REGISTRATION"]
        mensaje["tipoMensaje"] = "TDI*P"
        mensaje["fechaProceso"] = self._context.ahora
        
        if self._context.debug == mensaje["idDispositivo"]:
            mensaje["dbgMascara"] = self._dispositivo["maskBin"]
            mensaje["dbgMensaje"] = texto
              
        self._eliminar_campos(campos)

        if ",texto a buscar," in texto:
            log.info(texto)
        
        self._01_identificador(campos, mensaje)             # 01
        self._02_fecha_hora(campos, mensaje)                # 02
        self._03_fecha_hora1(campos, mensaje)               # 03
        self._04_datos_gps(campos, mensaje)                 # 04
        self._05_pt100_internas(campos, mensaje)            # 05
        
        self._06_pt100_externas(campos, mensaje)            # 06
        self._07_entradas_analogicas(campos, mensaje)       # 07
        self._08_apache(campos, mensaje)                    # 08 apache
        self._09_transcan(campos, mensaje)                  # 09
        self._10_transcan_advance(campos, mensaje)          # 10 Transcan Advance

        self._11_euroscan(campos, mensaje)                  # 11
        self._12_datacold(campos, mensaje)                  # 12       
        self._13_touchprint(campos, mensaje)                # 13
        self._14_digitales(campos, mensaje)                 # 14
        self._15_ibox(campos, mensaje)                      # 15

        self._16_hwasung_termo(campos, mensaje)             # 16 hwasung thermo
        self._17_carrier(campos, mensaje)                   # 17
        self._18_carrier_3rd_party(campos, mensaje)         # 18 Carrier third party
        self._19_carrier_partner(campos, mensaje)           # 19 Carrier partner
        self._20_das(campos, mensaje)                       # 20
        
        self._21_thermo_guard_vi(campos, mensaje)           # 21
        self._22_th12online(campos, mensaje)                # 22
        self._23_datos_gps(campos, mensaje)                 # 23
        self._24_contador(campos, mensaje)                  # 24
        self._25_mantenimiento(campos, mensaje)             # 25
        
        self._26_canbus(campos, mensaje)                    # 26
        self._27_canbus_horas(campos, mensaje)              # 27
        self._28_canbus_fuel(campos, mensaje)               # 28
        self._29_canbus_extendido(campos, mensaje)          # 29
        self._30_canbus_fms3(campos, mensaje)               # 30

        self._31_glp_iveco_euro5(campos, mensaje)           # 31
        self._32_knorr(campos, mensaje)                     # 32
        self._33_haldex(campos, mensaje)                    # 33
        self._34_wabco(campos, mensaje)                     # 34
        self._35_7080(campos, mensaje)                      # 35

        self._36_informacion_gsm(campos, mensaje)           # 36
        self._37_id_movil_slave(campos, mensaje)            # 37
        self._38_master(campos, mensaje)                    # 38
        self._39_trailer_7080(campos, mensaje)              # 39
        self._40_ibutton(campos, mensaje)                   # 40

        self._41_palfinger(campos, mensaje)                 # 41
        self._42_ns_carrier(campos, mensaje)                # 42
        self._43_conductor(campos, mensaje)                 # 43
        self._44_doble_conductor(campos, mensaje)           # 44
        self._45_alarma_puerta_slave(campos, mensaje)       # 45

        self._46_lls20160(campos, mensaje)                  # 46
        self._47_salidas_digitales(campos, mensaje)         # 47
        self._48_bloque_almacenamiento(campos, mensaje)     # 48
        self._49_power_supply(campos, mensaje)              # 49
        self._50_intelliset(campos, mensaje)                # 50
        
        self._99_finales(campos, mensaje)
        
        return mensaje