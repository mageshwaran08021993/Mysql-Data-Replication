from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd

# jdbc:redshift://test-work.432069170121.eu-north-1.redshift-serverless.amazonaws.com:5439/dev
REDSHIFT_HOST="test-work.432069170121.eu-north-1.redshift-serverless.amazonaws.com"
REDSHIFT_USER="admin"
REDSHIFT_PASS="AWSMagesh1"
REDSHIFT_PORT="5439"
REDSHIFT_DB="dev"
# redshift_engine = create_engine(f'postgresql://{REDSHIFT_USER}:{REDSHIFT_PASS}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}')
# df_read = pd.read_sql('SELECT * FROM first_rs;', redshift_engine)
# print(df_read)

from sqlalchemy import create_engine
import pandas as pd
import psycopg2
from sqlalchemy import text

conn = create_engine(f'postgresql://{REDSHIFT_USER}:{REDSHIFT_PASS}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}')


df = pd.DataFrame([{'Name': 'Ram', 'Age': 50},
                   {'Name': 'Bhim', 'Age': 23},
                   {'Name': 'Shyam'}])

df.to_sql('first_rs', conn, index=False, if_exists='replace')
# query = """select * from first_rs"""
# df1=pd.DataFrame(conn.connect().execute(query))
# print(df1.head())
# {REDSHIFT_USER}:{REDSHIFT_PASS}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}
# def connect(query):
#   conn = psycopg2.connect(
#       host=REDSHIFT_HOST,
#       port=REDSHIFT_PORT,
#       user=REDSHIFT_USER,
#       password=REDSHIFT_PASS,
#       dbname=REDSHIFT_DB
#   )
#   cur = conn.cursor()
#   cur.execute(query)
#   conn.commit()
#   cur.close()
#   conn.close()