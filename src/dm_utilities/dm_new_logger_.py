import logging
from logging import config

log_config = {
  "version":1,
  "root":{
    "handlers" : ["console"],
    "level": "DEBUG"
  },
  "handlers":{
    "console":{
      "formatter": "std_out",
      "class": "logging.StreamHandler",
      "level": "DEBUG"
    },
    "warnings_handler":{
      "formatter":"std_out",
      "class":"logging.FileHandler",
      "filters": ["warnings"],
      "level":"DEBUG",
      "filename":"warnings_only.log"
    },
    "errors_handler":{
      "formatter":"std_out",
      "class":"logging.FileHandler",
      "filters": ["errors"],
      "level":"DEBUG",
      "filename":"errors_only.log"
    }
  },
  "formatters":{
    "std_out": {
      "format": "%(asctime)s : %(levelname)s : %(module)s : %(funcName)s : %(lineno)d : (Process Details : (%(process)d, %(processName)s), Thread Details : (%(thread)d, %(threadName)s))\nLog : %(message)s",
      "datefmt":"%d-%m-%Y %I:%M:%S"
    }
  }
}


config.dictConfig(log_config)
logger = logging.getLogger("root")

logger.info("This is an info message")

