# yaotp
Yet another open telemetry processor

`yaotp` es un procesador genérico de telemetería, aunque la implementación
inicial es para dispositivos de la familia TH del fabricante español
[Técnicas de Ingeniería, S.L](https://gesinflot.com/) y su protocolo
T-Mobility.

#### ¿Cómo funciona?

El sistema está concebido para procesar colas de comunicaciones. Cada cola
está procesada por un _worker_ y, a su vez, cada _worker_ realiza las 
siguientes tareas:

- Recupera los mensajes de la cola
- Decodifica los valores aplicando un _parser_ específico
- Persiste los valores decodificados en orden en una colección MongoDB
- Reenvía (opcional) una señalización a través de RabbitMQ.

#### Qué información ofrece

Por el momento se está ofreciendo la siguiente información, dependiendo -eso
sí- de si el dispositivo va montado sobre una tractora o sobre un remolque.

- Información GPS completa
- CANBUS 
- TACÓGRAFO 
- EBS (*)
- FRIGORÍFICO (**)
- Alarmas varias (*)

(*) depende de la configuración del equipo externo recibimos una información
u otra.
(**) Hay un juego de información común (máquina y sondas), pero luego cada 
fabricante permite un diferente nivel de integración. Depende, pues, del
equipo externo que lleve equipado el remolque.

#### Instalación

Se puede hacer _checkout_ del programa en cualquier directorio. Aún no está
preparado para ser desplegado con `pip`, pero lo será. En el directorio del
usuario bajo el que se va a ejecutar crearemos el directorio `yaotp` que, 
a su vez, contendrá los directorios `etc` y `log`.

#### El archivo yaopt.config

Las secciones y variables que debe contener son las siguientes y no requieren
especial explicación salvo `debug`. Si se activa debug (int) para un 
dispositivo, el procesador guardará con cada mensaje decodificado el mensaje
original y la máscara correspondiente.

```
[MONGO]
uri=
db=
debug=-1

[TDI]
user=
password=
cola=
url_formatos=
url_ws=
```

#### Rotación de logs

Cada _worker_ deja un archivo de log con su mismo nombre. De momento no hace
autorotación, así que es conveniente configurar `logrotate` para ir limpiando.

#### Reenvío de eventos

`yaotp` puede reenviar eventos a través de una cola RabbitMQ. Si no se quiere
desplegar una cola de mensajes se puede trabajar también consultando
directamente la base de datos (`_id` preserva la ordenación de llegada).

#### Requerimientos máquina

El consumo de recursos de memoria y CPU es mínimo. Puede desplegarse en las
instancias más pequeñas de Amazon AWS o de Azure y siempre va a ir sobrado.
De hecho puede desplegarse en cualquier máquina en la que "sobre" un poco de
potencia de procesador.

