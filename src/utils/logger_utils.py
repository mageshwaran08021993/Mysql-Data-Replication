import logging
import sys
from logging import handlers
import socket
import os
import json
from typing import Dict

file_formatter =  logging.Formatter("%(asctime)s | %(levelname)s | %(component_name)s | %(host)s | %(message)s | %(extended_message)s")
console_formatter =  logging.Formatter("%(asctime)s | %(levelname)s | %(component_name)s | %(host)s | %(message)s | %(extended_message)s")

HOSTNAME = socket.gethostname()
FILE_LOG_LEVEL = logging.DEBUG
CONSOLE_LOG_LEVEL = logging.INFO
ROOT_LOGGING_LEVEL = logging.DEBUG
FILE_NAME_EXTENSION = ".log"
BACKUP_COUNT = 30
ENCODING = "utf8"
console_stream = sys.stdout
logger_map: Dict[str, int] = {}

class Logger:
    log_instnace = None
    def __init__(self, component_name, log_to_console, log_to_file, log_name, file_path):
        self.component_name = component_name
        self.log_to_console = log_to_console
        self.log_to_file = log_to_file
        self.log_name = log_name
        self.file_path = file_path
        self.create_log_instance()

    def create_log_instance(self):
        log_name = self.log_name
        self.logger = logging.getLogger(log_name)
        if self.log_to_file:
            if not os.path.exists(self.file_path):
                os.makedirs(self.file_path)

            filehandler = handlers.TimedRotatingFileHandler(
                filename=self.file_path + log_name + FILE_NAME_EXTENSION,
                when="D",
                interval=1,
                backupCount=BACKUP_COUNT,
                encoding=ENCODING
            )
            filehandler.setFormatter(file_formatter)
            filehandler.setLevel(FILE_LOG_LEVEL)
            self.logger.addHandler(filehandler)

        if self.log_to_console:
            consolehandler = logging.StreamHandler(stream=console_stream)
            consolehandler.setFormatter(console_formatter)
            consolehandler.setLevel(CONSOLE_LOG_LEVEL)
            self.logger.addHandler(consolehandler)

        self.logger.setLevel(ROOT_LOGGING_LEVEL)

    def save_log(self, level, component_name, message, event_type = "", extended_message=""):

        log = logging.LoggerAdapter(
            self.logger, {
                "host": HOSTNAME,
                "component_name": component_name,
                "event_type": event_type,
                "extended_message": extended_message,
            }
        )
        if level.lower() == "error":
            log.error(message)
        elif level.lower() == "info":
            log.info(message)
        elif level.lower() == "debug":
            log.debug(message)
        else:
            log.error("Logging level should be one of [ERROR | INFO | DEBUG]")

    @staticmethod
    def get_logger_instance(component_name, log_to_console, log_to_file, log_name, file_path):
        if Logger.log_instnace is None:
            Logger.log_instnace = Logger(
                component_name=component_name,
                log_to_console=log_to_console,
                log_to_file=log_to_file,
                log_name=log_name,
                file_path=file_path,
            )
        return Logger.log_instnace
#
# get_log.save_log(level="info", component_name= "", event_type="Write", message="Checck", extended_message="Ext_check")