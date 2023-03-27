import unittest
import src.utils.db_connect as db_utils

db_utils.Database(db_host = "magesh.106501636072.us-east-1.redshift-serverless.amazonaws.com",db_port = "5439", db_user = "admin", db_password = "AWSMagesh1", db_name = "dev",    db_schema = "public", database_type = "redshift")
db = db_utils.DatabaseUtils()
session = db.create_session()

class DeleteDataTestCls(unittest.TestCase):
    def test_delete_int_data(self):
       db.delete_data("tbl_employees", json_data={"id": 1 })
       self.assertEqual(True, False)  # add assertion here

    def test_delete_str_data(self):
       data =  {'id': '1', 'empname': 'EMP_1', 'emploc': 'UAE', 'empsal': '15000.0',
         'empadd': '{"H.No": "0-91/554", "email": "EMP_1@gmail.com", "country": "UAE", "zipcode": "114536"}',
         'empdesignation': 'Manager', 'is_manager': '1', 'is_teamlead': '0', 'dateofjoin': '2022-11-17 00:00:00',
         'Lastdate': 'None'}
       status = db.delete_data("tbl_employees", json_data={"id":9989627, 'empname': 'EMP_998967'})
       self.assertEqual(True, status)  # add assertion here

    # Json is not available in Redshift, it would be string
    def test_delete_json_data(self):
       delete_data = {}
       delete_data["empadd"] = '{\"H.No\": \"0-41/553\", \"email\": \"EMP_998967@gmail.com\", \"country\": \"UAE\", \"zipcode\": \"81438\"}'
       delete_data["id"] = 123
       status = db.delete_data("tbl_employees", json_data=delete_data)
       self.assertEqual(True, status)  # add assertion here

    def test_delete_datetime_data(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       delete_data = {}
       delete_data["dateofjoin"] = '2022-11-20 00:00:00'
       delete_data["id"] = 12
       status = db.delete_data("tbl_employees", json_data=delete_data)
       self.assertEqual(True, status)  # add assertion here

    def test_delete_null_data(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       delete_data = {}
       delete_data["Lastdate"] = None
       delete_data["id"] = 123
       try:
            status = db.delete_data("tbl_employees", json_data=delete_data)
            status = True
       except:
            status = False
       self.assertEqual(True, status)  # add assertion here

    def test_delete_wrong_datatype_null_data_exception(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       delete_data = {}
       delete_data["Lastdate"] = 'None'
       delete_data["id"] = 123
       try:
            status = db.delete_data("tbl_employees", json_data=delete_data)
            status = True
       except:
            status = False
       self.assertEqual(False, status)

    def test_delete_wrong_data_for_float_datatype_field_exception(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       delete_data = {}
       delete_data["empsal"] = 'test'
       delete_data["id"] = 123
       try:
            status = db.delete_data("tbl_employees", json_data=delete_data)
            status = True
       except:
            import traceback
            print(traceback.format_exc())
            status = False
       self.assertEqual(False, status)

    def test_delete_Data_with_all_columns(self):
       # 'dateofjoin': '2022-11-17 00:00:00', 'Lastdate': 'None'
       delete_data = {'id': 11121012, 'empname': 'EMP_1', 'emploc': 'UAE', 'empsal': 15000.0,
                   'empadd': '[false, \\"None - Table name is Invalid\\"]', 'empdesignation': 'Manager',
                   'is_manager': 1, 'is_teamlead': 0, 'dateofjoin': '2022-11-17 00:00:00'}

       try:
            status = db.delete_data("tbl_employees", json_data=delete_data)
            status = True
       except:
            import traceback
            print(traceback.format_exc())
            status = False
       self.assertEqual(True, status)

if __name__ == '__main__':
    unittest.main()
