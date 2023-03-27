from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

engine = create_engine('postgresql+psycopg2://username:password@host:port/database')

metadata = MetaData()

# my_table = Table('my_table', metadata,
#                  Column('id', Integer, primary_key=True),
#                  Column('name', String),
#                  Column('age', Integer)
#                  )

data = {'id': 1, 'name': 'John', 'age': 30}

insert_stmt = my_table.insert().values(data)

with engine.connect() as conn:
    conn.execute(insert_stmt)
