import unittest
import src.db_connect as db_utils

db_utils.Database(db_schema="public", database_type="redshift")
db = db_utils.DatabaseUtils()
session = db.create_session()

class MyTestCase(unittest.TestCase):
    def test_update_int_data(self):
       data =  {'id': '1', 'empname': 'EMP_1', 'emploc': 'UAE', 'empsal': '15000.0',
         'empadd': '{"H.No": "0-91/554", "email": "EMP_1@gmail.com", "country": "UAE", "zipcode": "114536"}',
         'empdesignation': 'Manager', 'is_manager': '1', 'is_teamlead': '0', 'dateofjoin': '2022-11-17 00:00:00',
         'Lastdate': 'None'}
       db.update_data("tbl_employees", data_json={"id":9989627}, old_data_json={"id": 999, "empname":"ma"})
       self.assertEqual(True, False)  # add assertion here

    def test_update_str_data(self):
       data =  {'id': '1', 'empname': 'EMP_1', 'emploc': 'UAE', 'empsal': '15000.0',
         'empadd': '{"H.No": "0-91/554", "email": "EMP_1@gmail.com", "country": "UAE", "zipcode": "114536"}',
         'empdesignation': 'Manager', 'is_manager': '1', 'is_teamlead': '0', 'dateofjoin': '2022-11-17 00:00:00',
         'Lastdate': 'None'}
       status = db.update_data("tbl_employees", old_data_json={"id":9989627, 'empname': 'EMP_998967'}, data_json={"id": 999, "empname":"ma"})
       self.assertEqual(True, status)  # add assertion here

    # Json is not available in Redshift, it would be string
    def test_update_json_data(self):
       get_data = {}
       get_data["empadd"] = '{\"H.No\": \"0-41/553\", \"email\": \"EMP_998967@gmail.com\", \"country\": \"UAE\", \"zipcode\": \"81438\"}'
       get_data["id"] = 123
       status = db.update_data("tbl_employees", old_data_json={"id": 111}, data_json={"id": 12, "empadd": '{\"H.No\": \"0-41/553\"}'})
       self.assertEqual(True, status)  # add assertion here

    def test_update_datetime_data(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       get_data = {}
       get_data["dateofjoin"] = '2022-11-20 00:00:00'
       get_data["id"] = 12
       status = db.update_data("tbl_employees", old_data_json=get_data, data_json={"id": 123, "empadd": '{\"H.No\": \"0-41/553\"}'})
       self.assertEqual(True, status)  # add assertion here

    def test_update_null_data(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       get_data = {}
       get_data["Lastdate"] = None
       get_data["id"] = 123
       try:
            status = db.update_data("tbl_employees", old_data_json=get_data, data_json={"id": 111, "lastdate": '2022-11-20 00:00:00'})
            status = True
       except:
            status = False
       self.assertEqual(True, status)  # add assertion here

    def test_update_wrong_datatype_null_data_exception(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       get_data = {}
       get_data["Lastdate"] = 'None'
       get_data["id"] = 123
       try:
            status = db.update_data("tbl_employees", old_data_json=get_data, data_json={"id": 111, "lastdate": '2022-11-20 00:00:00'})
            status = True
       except:
            status = False
       self.assertEqual(False, status)

    def test_update_wrong_data_for_float_datatype_field_exception(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       get_data = {}
       get_data["empsal"] = 'test'
       get_data["id"] = 123
       try:
            status = db.update_data("tbl_employees", old_data_json={"id": 111}, data_json=get_data)
            status = True
       except:
            import traceback
            print(traceback.format_exc())
            status = False
       self.assertEqual(False, status)

    def test_update_with_all_columns(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       new_data = {'id': 11121012, 'empname': 'EMP_1', 'emploc': 'UAE', 'empsal': 15000.0,
                   'empadd': '[false, \\"None - Table name is Invalid\\"]', 'empdesignation': 'Manager',
                   'is_manager': 1, 'is_teamlead': 0, 'dateofjoin': '2022-11-17 00:00:00'}
       old_data = {'id': 1, 'empname': 'EMP_1', 'emploc': 'UAE', 'empsal': 15000.0,
               'empadd': '{\\"H.No\\": \\"0-41/553\\", \\"email\\": \\"EMP_998967@gmail.com\\", \\"country\\": \\"UAE\\", \\"zipcode\\": \\"81438\\"}', 'empdesignation': 'Senior-Software-Engineer', 'is_manager': 0,
               'is_teamlead': 0, 'dateofjoin': '2022-11-17 00:00:00'}
       try:
            status = db.update_data("tbl_employees", old_data_json=old_data, data_json=new_data)
            status = True
       except:
            import traceback
            print(traceback.format_exc())
            status = False
       self.assertEqual(True, status)

if __name__ == '__main__':
    unittest.main()
