#!/usr/bin/python3
# -*- coding: utf-8 -*-

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