import unittest
import src.utils.db_connect as db_utils

db_utils.Database(db_host = "magesh.106501636072.us-east-1.redshift-serverless.amazonaws.com",db_port = "5439", db_user = "admin", db_password = "AWSMagesh1", db_name = "dev",    db_schema = "public", database_type = "redshift")
db = db_utils.DatabaseUtils()
session = db.create_session()

class InsertDataTestCls(unittest.TestCase):
    def test_add_data_int_data(self):
       db.add_data("tbl_employees", json_data={"id": 1 })
       self.assertEqual(True, False)  # add assertion here

    def test_add_data_str_data(self):
       data =  {'id': '1', 'empname': 'EMP_1', 'emploc': 'UAE', 'empsal': '15000.0',
         'empadd': '{"H.No": "0-91/554", "email": "EMP_1@gmail.com", "country": "UAE", "zipcode": "114536"}',
         'empdesignation': 'Manager', 'is_manager': '1', 'is_teamlead': '0', 'dateofjoin': '2022-11-17 00:00:00',
         'Lastdate': 'None'}
       status = db.add_data("tbl_employees", json_data={"id":9989627, 'empname': 'EMP_998967'})
       self.assertEqual(True, status)  # add assertion here

    # Json is not available in Redshift, it would be string
    def test_add_data_json_data(self):
       add_data = {}
       add_data["empadd"] = '{\"H.No\": \"0-41/553\", \"email\": \"EMP_998967@gmail.com\", \"country\": \"UAE\", \"zipcode\": \"81438\"}'
       add_data["id"] = 123
       status = db.add_data("tbl_employees", json_data=add_data)
       self.assertEqual(True, status)  # add assertion here

    def test_add_data_datetime_data(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       add_data = {}
       add_data["dateofjoin"] = '2022-11-20 00:00:00'
       add_data["id"] = 12
       status = db.add_data("tbl_employees", json_data=add_data)
       self.assertEqual(True, status)  # add assertion here

    def test_add_data_null_data(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       add_data = {}
       add_data["Lastdate"] = None
       add_data["id"] = 123
       try:
            status = db.add_data("tbl_employees", json_data=add_data)
            status = True
       except:
            status = False
       self.assertEqual(True, status)  # add assertion here

    def test_add_data_wrong_datatype_null_data_exception(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       add_data = {}
       add_data["Lastdate"] = 'None'
       add_data["id"] = 123
       try:
            status = db.add_data("tbl_employees", json_data=add_data)
            status = True
       except:
            status = False
       self.assertEqual(False, status)

    def test_add_data_wrong_data_for_float_datatype_field_exception(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       add_data = {}
       add_data["empsal"] = 'test'
       add_data["id"] = 123
       try:
            status = db.add_data("tbl_employees", json_data=add_data)
            status = True
       except:
            import traceback
            print(traceback.format_exc())
            status = False
       self.assertEqual(False, status)

    def test_add_data_with_all_columns(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       add_data = {'id': 11121012, 'empname': 'EMP_1', 'emploc': 'UAE', 'empsal': 15000.0,
                   'empadd': '[false, \\"None - Table name is Invalid\\"]', 'empdesignation': 'Manager',
                   'is_manager': 1, 'is_teamlead': 0, 'dateofjoin': '2022-11-17 00:00:00'}

       try:
            status = db.add_data("tbl_employees", json_data=add_data)
            status = True
       except:
            import traceback
            print(traceback.format_exc())
            status = False
       self.assertEqual(True, status)

if __name__ == '__main__':
    unittest.main()
