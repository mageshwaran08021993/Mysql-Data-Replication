

from pymysqlreplication.row_event import DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent
from src.utils.db_connect import Database, DatabaseUtils
from src.utils.logger_utils import Logger
import json
import datetime
import os
from traceback import format_exc
from src.config import Config
from src.mysql_event import MysqlEventsReplica

db = None
get_logger = None

def redshift_intialization():
    global db
    if db is None:
        print("Inside db connect")
        Database(db_schema=Config.get_target_db_schema(),
                 database_type=Config.get_target_database_type(),
                 db_host=Config.get_target_db_host(),
                 db_name=Config.get_target_db_name(),
                 db_port=Config.get_target_db_port(),
                 db_user=Config.get_target_db_user(),
                 db_password=Config.get_target_db_password())
        db = DatabaseUtils()
    # session = db.create_session()
    return db

def logger_intialization():
    # get_logger = globals()["get_logger"]
    global get_logger
    if get_logger is None:
        get_logger = Logger.get_logger_instance(component_name="", log_to_file=Config.get_log_to_file(), log_to_console=Config.get_log_to_console(), log_name=Config.get_log_file_name(), file_path=Config.get_log_file_path())
    return get_logger

def start_stream():
    try:
        logger = logger_intialization()
        logger.save_log(level="info", component_name="Replica-start_stream", message="Stream started", extended_message="")
        stream = MysqlEventsReplica.get_stream_instance()
        replica_obj = MysqlEventsReplica()
        redshift_obj = redshift_intialization()
        for event in stream:
            try:
                if isinstance(event, DeleteRowsEvent):
                    logger.save_log(level="debug", component_name="Replica-start_stream", message="Delete event started", extended_message="")
                    table_name = event.table
                    timestamp = event.timestamp
                    log_pos = event.packet.log_pos
                    lt_json_data = event.rows
                    logger.save_log(level="info", component_name="Replica-start_stream", message="Delete event",
                                    extended_message=f"Delete Event for table - {table_name} and its log position - {log_pos}, its timestamp - {timestamp}")
                    logger.save_log(level="debug", component_name="Replica-start_stream", message="Delete event",
                                    extended_message=f"Delete Event for table - {table_name} and its log position - {log_pos}, its timestamp - {timestamp} and data - {lt_json_data}")
                    replica_obj.delete_event(table__name_val=table_name, json_data_lt=lt_json_data, redshift_object=redshift_obj)
                # continue
                if isinstance(event, UpdateRowsEvent):
                    logger.save_log(level="debug", component_name="Replica-start_stream", message="Update event started", extended_message="")
                    table_name = event.table
                    timestamp = event.timestamp
                    log_pos = event.packet.log_pos
                    lt_json_data = event.rows
                    logger.save_log(level="info", component_name="Replica-start_stream", message="Update event",
                                    extended_message=f"Update Event for table - {table_name}, its timestamp - {timestamp} and its log position - {log_pos}")
                    logger.save_log(level="debug", component_name="Replica-start_stream", message="Update event",
                                    extended_message=f"Update Event for table - {table_name}, its timestamp - {timestamp} and its log position - {log_pos} and data - {lt_json_data}")
                    try:
                        replica_obj.update_event(table_name_val=table_name, json_data_lt=lt_json_data,
                                             redshift_object=redshift_obj)
                    except Exception as e:
                        raise e

                if isinstance(event, WriteRowsEvent):
                    logger.save_log(level="debug", component_name="Replica-start_stream", message="Write event started",
                                    extended_message="")
                    table_name = event.table
                    timestamp = event.timestamp
                    log_pos = event.packet.log_pos
                    lt_json_data = event.rows
                    logger.save_log(level="info", component_name="Replica-start_stream", message="Insert event",
                                    extended_message=f"INSERT Event for table - {table_name} and its log position - {log_pos}, its timestamp - {timestamp}")
                    logger.save_log(level="debug", component_name="Replica-start_stream", message="Insert event",
                                    extended_message=f"INSERT Event for table - {table_name} and its log position - {log_pos}, its timestamp - {timestamp} and data - {lt_json_data}")
                    status, msg = replica_obj.insert_event(table_name_val=table_name, json_data_lt=lt_json_data,
                                             redshift_object=redshift_obj)
                    if status is not True:
                        logger.save_log(level="error", component_name="Replica-start_stream", message="Insert event",
                                        extended_message=f"{msg}")
                        # TODO: Alerts
            except Exception as e:
                # TODO - In the event of any exception, need to include Alerts
                tb_message = format_exc().replace('\n', '; ')
                logger.save_log(level="error", component_name="Replica-start_stream", message="Exception",
                                extended_message=f"{tb_message}")
        stream.close()
    except Exception as e:
        # TODO - In the event of any exception, need to include Alerts
        tb_message = format_exc().replace('\n', '; ')
        logger.save_log(level="error", component_name="Replica-start_stream", message="Exception",
                        extended_message=f"{tb_message}")


if __name__ == "__main__":
    start_stream()