from sqlalchemy import create_engine, text, column, Table
from sqlalchemy.orm import Session
import json

engine = create_engine(f'postgresql://admin:AWSMagesh1@test-work.432069170121.eu-north-1.redshift-serverless.amazonaws.com:5439/dev')
session = Session(bind=engine)

# construct a JSON object
json_data = {"id": "123", "empname": "EMP_998967"}

# extract the key-value pairs of the JSON object using json_extract()
json_data = {"id": 123, "name": "mage"}
assignments = [column(key) == value for key, value in json_data.items()]
print(assignments)

# update the table with the column assignments
session.query(Table("tbl_employees")).filter(assignments).scalar().all()

session.commit()
