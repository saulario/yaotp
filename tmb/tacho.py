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

import configparser
import logging
import os
import sys

import requests
import urllib
import uuid

YAOTP_HOME = ("%s/yaotp" % os.path.expanduser("~"))
YAOTP_CONFIG = ("%s/etc/yaotp.config" % YAOTP_HOME)
YAOTP_LOG = ("%s/log/%s.log" % 
             (YAOTP_HOME, os.path.basename(__file__).split(".")[0]))
logging.basicConfig(level=logging.INFO, filename=YAOTP_LOG,
                    format="%(asctime)s %(levelname)s %(module)s.%(funcName)s %(message)s")    
log = logging.getLogger(__name__)

if __name__ == "__main__":
    log.info("-----> Inicio **********")
    retval = 0
    
    try:
        cp = configparser.ConfigParser()
        cp.read(YAOTP_CONFIG)
               
#        url = ("%s/listvehics" % cp.get("TDI", "url_ws"))
#        res = requests.get(url)
        
        
        url = ("%s/tdi/AMMForm?" % cp.get("TDI", "url_formatos"))
        
        payload = {}
        payload["pushTarget"] = "GPRSSocketCommFE"
        payload["content"] = "GPRS/SOCKET,%s,%d,%s,%s,%s" % (
                uuid.uuid4(),
                21142,
                "5906560794897",
                "sese012127@orange.es",
                "TDI*BATGETINFOOO")
        
        res = requests.get(url, params=payload, auth = (
                cp.get("TDI", "user"),
                cp.get("TDI", "password")))
        
        
        
        
        
    except Exception as e:
        log.error(e)
        retval = 1
    finally:
        pass
    
    log.info("<----- Fin")
    sys.exit(retval)