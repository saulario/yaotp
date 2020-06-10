import datetime
import json

if __name__ == "__main__":

    d = {}
    d["cadena"] = "cadena"
    d["compuesto"] = {}
    d["compuesto"]["entero"]  = 100
    d["compuesto"]["decimal"]  = 100.25
    d["compuesto"]["fecha"]  = str(datetime.date.today())
    d["compuesto"]["fecha_hora"]  = str(datetime.datetime.now())
    d["timedelta"]  = str(datetime.datetime.now() -  datetime.datetime.utcnow())
    d["time"]  = str(datetime.datetime.now().time())


    a = json.dumps(d)
    print(a)