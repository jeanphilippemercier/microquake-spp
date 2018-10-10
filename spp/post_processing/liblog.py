import logging

'''
    MTH: The whole point of this is to use one logger for an entire script + libs and
         ensure the logger handlers only get configured once

    Note: I don't use <name> in the format - e.g., logging.getLogger(__name__) since
          I would have to do this at every level in the module & this still wouldn't give me the 
          current def() name in the msg!
'''

# These are just used to verify optional setLevel arg
LOGLEVELS = ['CRITICAL','ERROR','WARN','INFO','DEBUG','NOTSET']

# logging.getLogger() will return the root logger of the parent calling func
#   if it wasn't configured (no handlers) then we'll add a default console handler
#   and optionally a file handler

#def get_log_level():
    #ch = logging.StreamHandler()
    #return ch.getEffectiveLevel()
    #return(get_log_level(logger.getEffectiveLevel()))

def getLogger(*args, **kwargs):
    fname = 'getLogger'
    logger = logging.getLogger()

    if len(logger.handlers) == 0:

        if '-t' in args or 'show_time' in kwargs.keys() and kwargs['show_time']:
            #formatter = logging.Formatter('%(asctime)s [%(levelname)5s] <%(name)s> %(message)s')
            formatter = logging.Formatter('%(asctime)s [%(levelname)5s] %(message)s')
        else:
            formatter = logging.Formatter('[%(levelname)s] %(message)s')
            #formatter = logging.Formatter('[%(levelname)5s] %(message)s')

        # log to stdout by default
        if 'log_stdout' in kwargs.keys() and not kwargs['log_stdout']: 
            pass
        else:
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            ch.setLevel(logging.INFO)
            logger.addHandler(ch)

        # optionally log to logfile
        if 'logfile' in kwargs.keys():
            fh = logging.FileHandler(kwargs['logfile'])
            fh.setFormatter(formatter)
            fh.setLevel(logging.DEBUG)
            logger.addHandler(fh)
            #print("Log to file:%s with DEBUG" % kwargs['logfile'])

        # Default logLevel - this will get passed to modules that look at root logLevel
        logger.setLevel(logging.WARN)

        # optionally set loglevel:
        if 'loglevel' in kwargs.keys():
            if kwargs['loglevel'] not in LOGLEVELS:
                logger.error("<%s.%s> Invalid loglevel:%s --> Choose from:%s" % \
                            (__name__,fname, kwargs['loglevel'], LOGLEVELS))
                exit(2)
            logger.setLevel(logging.getLevelName(kwargs['loglevel']))

    return logger

