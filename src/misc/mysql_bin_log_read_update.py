
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, TableMapEvent

mysql_settings = {"host": "localhost",
        "port": 3306,
        "user": "root",
        "passwd": "mypass"
        }
# resume_stream=True,
# log_pos=last_known_lsn
# blocking=True,
# only_events, ignored_events, filter_non_implemented_events
#                   QueryEvent,
                # RotateEvent,
                # StopEvent,
                # FormatDescriptionEvent,
                # XidEvent,
                # GtidEvent,
                # BeginLoadQueryEvent,
                # ExecuteLoadQueryEvent,
                # UpdateRowsEvent,
                # WriteRowsEvent,
                # DeleteRowsEvent,
                # TableMapEvent,
                # HeartbeatLogEvent,
                # NotImplementedEvent,
                # MariadbGtidEvent
stream = BinLogStreamReader(connection_settings = mysql_settings, server_id=100,only_events=[WriteRowsEvent, UpdateRowsEvent])
print("Inside Check")

# In order to get the data from last captured known log pos

# stream.log_pos
for binlogevent in stream:
    # print(f" Event - {type(binlogevent)} ------event type {binlogevent.event_type} -- table id --- {binlogevent._read_table_id}")
    # print(f"complete data - {binlogevent.dump()}")
    # print(f"log postion - {binlogevent.log_pos}")
    # if isinstance(binlogevent, QueryEvent):
    #     print("type - ", type(binlogevent.dump()))
    #     print("Query statement - ", binlogevent.query)
    #     if binlogevent.query.lower() != "commit":
    #         from redshift_connectivity import connect
    #         print("Inside Redshift block")
    #         # connect(binlogevent.query.replace('"',"'"))
    # event = {
    #     # "schema": binlogevent,
    #         #  "table": binlogevent.table,
    #          "event_type": binlogevent.}
    print(type(binlogevent))
    if isinstance(binlogevent, UpdateRowsEvent):
        print("Update worker")
        for row in binlogevent.rows:
            old_data = row["before_values"]
            new_data = row["after_values"]
            print(f"Before values - {old_data}")
            print(f"Update: table={binlogevent.table}, row={row['after_values']}")

    if isinstance(binlogevent, WriteRowsEvent):
        print("Inside WriteRowsEvent")
        rows=binlogevent.rows
        table_name = binlogevent.table
        insert_query = f"insert into {table_name} ($COLUMN_NAME) values ($COLUMN_VALUES)"
        for row in rows:
            columns=row.get("values")
            print(row.get("values"))
            # print(type(columns))
            # first=0
            # for col_name, col_value in columns.items():
            #     if first == 0:
            #         first=1
            #         COLUMN_NAME = col_name
            #         # COLUMN_VALUES.append(col_value)
            #         COLUMN_VALUES = col_value
            #         continue
            #     import json
            #     if col_name == "empadd":
            #         print(f"type - {type(col_value)}")
            #         # str_json = json.dumps(col_value)
            #         # col_value = json.loads(str_json.encode())
            #         new_dict = {}
            #         for k, v in col_value.items():
            #             new_dict[k.decode()]=v.decode()
            #         col_value=json.dumps(new_dict).replace('"', '\\"')
            #     COLUMN_NAME = str(COLUMN_NAME) + "," + str(col_name)
            #     # COLUMN_VALUES.append(col_value)
            #     COLUMN_VALUES = str(COLUMN_VALUES) + "," + str(col_value)
            #
            # # Complete_insert_query = insert_query.replace("$COLUMN_NAME", COLUMN_NAME).replace("$COLUMN_VALUES", COLUMN_VALUES)
            # Complete_insert_query = """INSERT INTO tbl_employees VALUES (9989627,'EMP_998967','UAE',12000,'{\"H.No\": \"0-41/553\", \"email\": \"EMP_998967@gmail.com\", \"country\": \"UAE\", \"zipcode\": \"81438\"}','Senior-Software-Engineer',0,0,'2022-11-20 00:00:00',NULL)"""
            # from redshift_connectivity import connect
            # print("Inside Redshift block")
            # connect(Complete_insert_query)
        # print(f"type - {type(a)}")
        # print()
        # print(binlogevent)
        # d = binlogevent.__dict__
        # for k, v in d.items():
        #     # if str(k) == "packet":
        #     #     print("Event -", v.event.__dict__)
        #     if str(k) == "_ctl_connection":
        #         print("CTL Result", v._result.__dict__)
        #     if not (isinstance(v,int) or isinstance(v, str) or isinstance(v,list) or isinstance(v, dict) or v is None or isinstance(v, bytes)):
        #         print(f"key - ", k)
        #         print(f"value - ", v)
        #         if not (isinstance(v,int) or isinstance(v, str) or isinstance(v,list) or isinstance(v, dict) or v is None or isinstance(v, bytes)):
        #             # print(v.__dict__)
        #             pass
        # print(f"type - {type(binlogevent)}")
        # print(f"{binlogevent.__dict__}")
        # # print(help(binlogevent))
        # print(binlogevent.schema)
        # print(binlogevent.table)
        
        # # columns = [binlogevent.columns(**col_def) for col_def in column_defs]
        # print(binlogevent.columns)
        # columns=binlogevent.columns

        # for column in columns:
        #     print("Column name:", column.name)
        #     print("Data type:", column.type)
        # print("Length:", column.length)
        # print("Flags:", column.flags)
    # if binlogevent.event_type in [WRITE_ROWS_EVENTv1, WRITE_ROWS_EVENTv2]:
    #     for row in binlogevent.rows:
    #         event["data"] = row["values"]
    #         print(event)
    # elif binlogevent.event_type in [UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2]:
    #     for row in binlogevent.rows:
    #         event["old_data"] = row["before_values"]
    #         event["new_data"] = row["after_values"]
    #         print(event)
    # elif binlogevent.event_type in [DELETE_ROWS_EVENTv1, DELETE_ROWS_EVENTv2]:
    #     for row in binlogevent.rows:
    #         event["data"] = row["values"]
    #         print(event)

print(f"loast log pos - {stream.log_pos}")
stream.close()
