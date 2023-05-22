import logging


class LogConfig:
    """ This class handles logging """

    log_date_format = "%y-%m-%d %H:%M:%S"
    log_format = "%(asctime)s  %(module)s : %(lineno)d [%(levelname)s] %(message)s"
    formatter = logging.Formatter(log_format, log_date_format)

    def __init__(self, file, debug=False):
        if debug:
            log_level = logging.DEBUG
        else:
            log_level = logging.INFO
        self.handler = logging.FileHandler(file)
        self._log_level = log_level

    def get_module_logger(self, mod_name=None):
        logger = logging.getLogger(mod_name)
        if logger.hasHandlers():
            while len(logger.handlers) > 0:
                h = logger.handlers[0]
                logger.removeHandler(h)

        logger.setLevel(self._log_level)
        self.handler.setFormatter(self.formatter)
        logger.addHandler(self.handler)

        return logger

    def close_handler(self):
        self.handler.flush()
        self.handler.close()
