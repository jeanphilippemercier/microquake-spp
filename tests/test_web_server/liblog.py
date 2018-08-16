import logging

def getLogger():
    logger = logging.getLogger()
    #if len(logger.handlers) == 0:
    if 1==1:
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s [%(levelname)7s] %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
      # Default logLevel - this will get passed to modules that look at root logLevel
        logger.setLevel(logging.WARN)
    else:
        print(logger.handlers)
    return logger
