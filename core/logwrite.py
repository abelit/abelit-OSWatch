# encoding: utf-8
'''
@project: __oswatch__
@modules: database.logwrite
@description:
@created: Jul 31, 2016

@author: abelit
@email: ychenid@live.com

@licence: GPL

'''

import os
import logging
import logging.config
import logging.handlers
import configparser

"""
Import customized modules
"""
from config import dbconfig

class LogWrite:
    """docstring for LogWrite"""
    def __init__(self, loglevel='debugLogger', logmessage='', logpath=dbconfig.log['logpath'],logconf=dbconfig.log['logconf']):
        super(LogWrite, self).__init__()
        self.logpath=logpath
        self.loglevel=loglevel
        self.logconf=logconf
        self.logmessage=logmessage

    def config_log(self):
        #Change logconf
        cf=configparser.ConfigParser()
        cf.read(self.logconf)
        cf.set("handler_fileHandler","args",'('+"'"+self.logpath+"'"+','+"'"+'a'+"'"+')')
        cf.set("handler_rotatingHandler","args",'('+"'"+self.logpath+"'"+','+"'"+'a'+"'"+',10*1024*1024, 5'+')')
        fp=open(self.logconf,'w')
        cf.write(fp)
        fp.close()

    def write_log(self):
        if os.path.isfile(self.logconf):
            self.config_log()
            # Read logging configuration
            logging.config.fileConfig(self.logconf)
            # Get logger name
            logger=logging.getLogger(self.loglevel)
            # Call lambda function to do switch statement
            result = {
                'debugLogger': lambda: logger.debug(self.logmessage),
                'infoLogger': lambda: logger.info(self.logmessage),
                'warnLogger': lambda: logger.warn(self.logmessage),
                'errorLogger': lambda: logger.error(self.logmessage)
            }[self.loglevel]()  
        else:
            # Configure logging parameters
            logging.basicConfig(
                level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='',
                filename=self.logpath,
                filemode='a'
                )
            # Log Rollback
            logging.handlers.RotatingFileHandler(self.logpath, maxBytes=10*1024*1024,backupCount=5)
            
            # Output log to screen
            console = logging.StreamHandler()
            console.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
            console.setFormatter(formatter)
            logging.getLogger('').addHandler(console)

            result = {
                'debugLogger': lambda:logging.debug(self.logmessage),
                'infoLogger': lambda:logging.info(self.logmessage),
                'warnLogger': lambda: logger.warn(self.logmessage),
                'errorLogger': lambda:logging.error(self.logmessage) 
            }[self.loglevel]()  

if __name__=='__main__':
    LogWrite(logmessage="This is a info message", loglevel='infoLogger').write_log()