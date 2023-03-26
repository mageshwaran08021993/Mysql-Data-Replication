
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, TableMapEvent
from src.db_connect import Database, DatabaseUtils
import json
import datetime

db = None
mysql_settings = {"host": "localhost",
        "port": 3306,
        "user": "root",
        "passwd": "mypass"
        }

def db_replica_instance_creation():
    global db
    if db is None:
        print("Inside db connect")
        Database(db_schema="public", database_type="redshift")
        db = DatabaseUtils()
    # session = db.create_session()
    return db

def make_data_compatible(json_data: dict, sub_check: str = "no", table_name_val: str = "None", db: DatabaseUtils = None, is_update_data:str = None):
    new_dict = {}
    for k, v in json_data.items():
        if isinstance(v, dict):
            v = make_data_compatible(v, sub_check="yes")
            if sub_check == "no":
                v = json.dumps(v).replace('"', '\\"')
        elif isinstance(v, bytes):
            v = v.decode()
        elif isinstance(v, datetime.datetime):
            v = v.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(v, str):
            v = str(v)
        if isinstance(k, bytes):
            k = k.decode()
        new_dict[k] = v
    print(type(db))
    if sub_check == "no":
        if db is None:
            print("DB is none")
            db = db_replica_instance_creation()
        status, new_dict = db.null_handling(table_val=table_name_val,json_data=new_dict, update_data=is_update_data)
        if status is not True:
            return False, new_dict
    return True, new_dict

stream = BinLogStreamReader(connection_settings = mysql_settings, server_id=100,only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent])
# In order to get the data from last captured known log pos

# db = db_replica_instance_creation()
# stream.log_pos
for binlogevent in stream:
    if isinstance(binlogevent, DeleteRowsEvent):
        print("Inside delete event")
        for row in event.rows:
            print("Delete row: %s" % row)
            # db_replica_instance_creation().update_data(table_val=table_name, old_data_json=old_data, data_json=new_data)
            # print("data inserted into table")
    continue
    if isinstance(binlogevent, UpdateRowsEvent):
        print("Update worker")
        for row in binlogevent.rows:
            old_data = row["before_values"]
            new_data = row["after_values"]
            print("new_data - ", new_data)
            print("old data - ", old_data)
            table_name = binlogevent.table
            status, old_data = make_data_compatible(old_data,db=db_replica_instance_creation(), table_name_val=table_name)
            status, new_data = make_data_compatible(new_data,db= db_replica_instance_creation(), table_name_val=table_name, is_update_data="yes")
            print("new_data - ", new_data)
            print("old data - ", old_data)
            # del old_data["Lastdate"]
            # del new_data["Lastdate"]
            db_replica_instance_creation().update_data(table_val=table_name, old_data_json=old_data, data_json=new_data)
            # print("data inserted into table")

    if isinstance(binlogevent, WriteRowsEvent):
        rows=binlogevent.rows
        table_name = binlogevent.table
        for row in rows:
            record_values=row.get("values")
            print("table_name - ", table_name)
            # json_data = make_data_compatible(record_values,db= db_replica_instance_creation(), table_name_val=table_name)
            # print("Record values - ", record_values)
            # Database(db_schema="public", database_type="redshift")
            # # a=Database.get_classes()
            # # tbl_employees = a.tbl_employees
            # db = DatabaseUtils()
            # db.add_data(table_val=table_name, json_data=json_data)
            print("data inserted into table")
print(f"loast log pos - {stream.log_pos}")
stream.close()
