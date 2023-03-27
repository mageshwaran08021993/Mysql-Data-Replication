from sqlalchemy import create_engine, MetaData, text, column
from sqlalchemy.ext.automap import automap_base
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
import pandas as pd

# TODO:
    # Schema changes update in database is not implemented
    # Multiple Updates in a single transaction is not implemented

class Database:
    test_db = None
    instance=None
    test_postgres = None
    session = None
    Base = None
    metadata = None
    schema = None
    database_type = None
    classes = None
    def __init__(self, db_host:str = "", db_port:str = "", db_user:str = "", db_password:str = "", db_name:str = "",    db_schema: str = "", database_type:str = ""):
        self.con = None
        # Database.schema = db_schema
        try:
            if database_type == "redshift":
                  params = {
                            'user': db_user,
                            'password': db_password,
                            'host': db_host,
                            'port': db_port,
                            'database': db_name
                        }
                  db = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
                  # db = create_engine(f'postgresql://admin:AWSMagesh1@test-work.432069170121.eu-north-1.redshift-serverless.amazonaws.com:5439/dev')

                  # Database.metadata = MetaData()
                  # Database.Base = automap_base(metadata = Database.metadata)
                  # Database.Base.prepare(db, reflect=True)
                  # metadata = MetaData()

                  # Reflect the schema of the database
                  # metadata.reflect(bind=db)
                  Base = automap_base()

                  # Reflect the database schema and generate table classes
                  Base.prepare(db, reflect=True)

                  # Access the reflected metadata
                  Database.metadata = Base.metadata
                  Database.classes = Base.metadata.tables
                  Database.instance = db
            elif database_type == "mysql":
                pass
        except Exception as e:
          import traceback
          print(traceback.format_exc())
          raise e

        

    @staticmethod
    def get_instance():
        # if Database.instance is None:
        #     Database(db_schema="public")
        return Database.instance


    @staticmethod
    def get_metadata():
        return Database.metadata

    @staticmethod
    def get_classes():
        if Database.instance is not None:
            return Database.classes



class DatabaseUtils:
    db = None
    session = None

    def __init__(self):
        self.db = Database.get_instance()
        # self.session = self.db.create_session()
 
    def create_session(self):
        try:
            Session = sessionmaker(bind=self.db)
            self.session = Session()
            return self.session
        except Exception as e:
            raise e
        
    def close_session(self):
        try:
            if self.session:
                self.session.close()
        except Exception as e:
            raise e
        
    def rollback_session(self):
        try:
            if self.session:
                self.session.rollback()
        except Exception as e:
            raise e
        

    def get_data(self, table_val, json_data):
        try:
            self.session = self.create_session()
            classes = Database.get_classes()
            table_name = classes.get(table_val, None)
            if table_name is None:
                return False, f"{table_val} - Table name is Invalid"
            base_query = self.session.query(table_name)
            for k, v in json_data.items():
                t = table_name.columns.get(k.lower(), None)
                if t is None:
                    return False, f"{k} Column name in table - {table_val} is Invalid"
                if v is None or v == 'None':
                    base_query = base_query.filter(t.is_(None))
                else:
                    base_query = base_query.filter(t == v)
            get_data_query = base_query
            get_data_val = self.session.execute(get_data_query).fetchall()
            print("Before Get data")
            for row in get_data_val:
                print(row)
            return True, get_data_query
        except Exception as e:
            raise e
        finally:
            self.close_session()

    # TODO
        # In ORM, need to do more analysis on dynamic mapping

    def add_data(self, table_val, json_data):
        try:
            # session = self.create_session()
            self.session = self.create_session()
            classes = Database.get_classes()
            table_name = classes.get(table_val, None)
            if table_name is None:
                raise Exception(f"{table_val} - Table name is Invalid")
            df = pd.DataFrame([json_data])
            df.to_csv(r"C:\Users\nithi\VS_CODE\MYSQL-REDSHIFT-PIPELINE\src\test.csv")
            df.to_sql(table_val, self.db, index=False, if_exists="append")
            self.session.flush()
            self.session.commit()
        except Exception as e:
            self.rollback_session()
            raise e
        finally:
            self.close_session()
        

    def update_data(self, table_val, data_json, old_data_json):
        try:
            self.session = self.create_session()
            status, get_data = self.get_data(table_val, old_data_json)
            if status is not True:
                raise Exception(get_data)
            # To update the data
            get_data.update(data_json)
            self.session.flush()
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        finally:
            self.session.close()
        
    def delete_data(self, table_val, json_data):
        try:
            self.session = self.create_session()
            status, get_data = self.get_data(table_val, json_data)
            if status is not True:
                raise Exception(get_data)
            get_data.delete()
            self.session.flush()
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        finally:
            self.session.close()

    def null_handling(self, table_val, json_data, update_data:str = None):
        try:
            self.session = self.create_session()
            classes = Database.get_classes()
            table_name = classes.get(table_val, None)
            if table_name is None:
                return False, f"{table_val} - Table name is Invalid"
            base_query = self.session.query(table_name)
            column_names_dt_lt = [(str(column.name).lower(), str(column.type).lower()) for column in table_name.columns]
            column_names_dt_dict = dict(column_names_dt_lt)

            new_data = {}
            for key, value in json_data.items():
                if column_names_dt_dict.get(str(key).lower(), None) is None:
                    return False, f"{key} Column name in table - {table_val} is Invalid"
                if column_names_dt_dict[str(key).lower()].__contains__("int") or column_names_dt_dict[str(key).lower()].__contains__("timestamp") or column_names_dt_dict[key].lower().__contains__("date"):
                    if not (update_data and (value == 'None' or value is None)):
                        new_data[key] = value
                else:
                    new_data[key] = value
                # To handle Null values while inserting
            return True, new_data
        except Exception as e:
            self.session.rollback()
            raise e
        finally:
            self.session.close()


if __name__ == "__main__":
    Database(db_schema="public", database_type="redshift")
    # a=Database.get_classes()
    # tbl_employees = a.tbl_employees
    db = DatabaseUtils()
    session = db.create_session()
    # db.get_data("tbl_employees", {"id": 1, "empname": "test"})
    data={'id': 111, 'empname': 'EMP_998967', 'emploc': 'UAE', 'empsal': 12000.0, 'empadd': 'test', 'empdesignation': 'Senior-Software-Engineer', 'is_manager': 0, 'is_teamlead': 0, 'dateofjoin': '2022-11-20', 'Lastdate': None}
    db.add_data("tbl_employees", data)
    # db.add_data("tbl_employees", {"id": 1, "empname": "test", "emploc": "chn"})
    # db.update_data("tbl_employees", data_json={"id":9989627}, old_data_json={"id": 999, "empname":"ma"})
    # db.delete_data("tbl_employees", {"id": 9989627, "empname": "test"})
    # get_data = (session.query(tbl_employees))
    # get_data_result = session.execute(get_data).scalars().all()
    # for row in get_data_result:
    #     print(row.id, row.empadd)
