import json
import datetime
from src.utils.db_connect import DatabaseUtils

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
    if sub_check == "no":
        # if db is None:
        #     db = redshift_intialization()
        status, new_dict = db.null_handling(table_val=table_name_val,json_data=new_dict, update_data=is_update_data)
        if status is not True:
            return False, new_dict
    return True, new_dict
