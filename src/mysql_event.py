from pymysqlreplication import BinLogStreamReader
from src.mysql_connection import mysql_connection_details
from src.data_formatter import make_data_compatible
from traceback import format_exc

class MysqlEventsReplica:

    stream = None
    def __init__(self):
        mysql_con = mysql_connection_details()
        MysqlEventsReplica.stream = BinLogStreamReader(connection_settings=mysql_con,
                                    server_id=100,
                                    blocking= True,
                                    resume_stream=True,
                                    log_pos=2769
                                                       )

    def delete_event(self, table__name_val: str, json_data_lt: list[dict], redshift_object):
        for row in json_data_lt:
            data = row.get("values")
            status, json_data = make_data_compatible(table_name_val=table__name_val,json_data=data,db=redshift_object)
            if not status:
                return False, json_data
            try:
                redshift_object.delete_data(table_val=table__name_val, json_data=json_data)
                return True, "Success"
            except Exception as e:
                return False, format_exc().replace('\n', '; ')

    def update_event(self, table_name_val: str, json_data_lt: list[dict], redshift_object):
        for row in json_data_lt:
            old_json_data = row["before_values"]
            new_json_data = row["after_values"]
            status, old_json_data = make_data_compatible(old_json_data, db=redshift_object, table_name_val=table_name_val)
            if not status:
                return False, old_json_data
            status, new_json_data = make_data_compatible(new_json_data, db=redshift_object, table_name_val=table_name_val,
                                                    is_update_data="yes")
            if not status:
                return False, old_json_data
            # try:
            redshift_object.update_data(table_val=table_name_val, old_data_json=old_json_data, data_json=new_json_data)
                # return True, "Success"
            # except Exception as e:
            #     return False, format_exc().replace('\n', '; ')

    def insert_event(self, table_name_val: str, json_data_lt: list[dict], redshift_object):
        for row in json_data_lt:
            json_data = row.get("values")
            status, json_data = make_data_compatible(json_data, db= redshift_object, table_name_val=table_name_val)
            if not status:
                return False, json_data
            try:
                redshift_object.add_data(table_val=table_name_val, json_data=json_data)
                return True, "Success"
            except Exception as e:
                return False, format_exc().replace('\n', '; ')
            # print("data inserted into table")

    @staticmethod
    def get_stream_instance():
        if MysqlEventsReplica.stream is None:
            MysqlEventsReplica()
        return MysqlEventsReplica.stream
