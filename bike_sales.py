import psycopg2
import pandas as pd
import config

# sql alchemy

# from sqlalchemy import create_engine
# from sqlalchemy import text
# from sqlalchemy import MetaData
# from sqlalchemy import Table, Column, Integer, String


# engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)

# with engine.connect() as conn:
#     result = conn.execute(text("select 'hello world'"))
#     print(result.all())

# metadata_obj = MetaData()
# user_table = Table(
#     "user_account",
#     metadata_obj,
#     Column('id', Integer, primary_key=True),
#     Column('name', String(30)),
#     Column('fullname', String)
# )



# Connection

CONN = psycopg2.connect(
    database="config.database",
    user="config.user", 
    password="config.password!", 
    host="config.host", 
    port="config.port"
    )

print("Database opened successfully")

### Each table as Pandas DataFrame

components = pd.read_sql('SELECT * FROM dsci_504.components', CONN)
customers = pd.read_sql('SELECT * FROM dsci_504.customers', CONN)
orders = pd.read_sql('SELECT * FROM dsci_504.orders', CONN)
products = pd.read_sql('SELECT * FROM dsci_504.products', CONN)
racs = pd.read_sql('SELECT * FROM dsci_504.racs', CONN)
states = pd.read_sql('SELECT * FROM dsci_504.states', CONN)
taxes = pd.read_sql('SELECT * FROM dsci_504.taxes', CONN)
warehouses = pd.read_sql('SELECT * FROM dsci_504.warehouses', CONN)
print(warehouses.head())
print(warehouses['warehouse_id'])


def merge_drop_dup(df1, df2, on, how):
    x = pd.merge(df1, df2, on = on, how = how, suffixes = ('_x', '_y'))
    cols = []
    for col in x.columns:
        cols.append(col)
    for col in cols:
        print(col)
        if '_x' in str(col):
            x[str(col).removesuffix('_x')] = x[col]
            x = x.drop(col, axis = 1)
        if '_y' in str(col):
            print('yes')
            x = x.drop(col, axis = 1)

    return x


# Joins

data = pd.merge(orders, customers, on = 'cus_id', how = 'outer')
data = merge_drop_dup(data, racs, 'cus_id', 'outer')
#data = pd.merge(data, racs, on = 'cus_id', how = 'outer')

print(data.head(1))
data = pd.merge(data, warehouses, on = 'warehouse_id', how = 'outer')
data = pd.merge(data, taxes, on = 'tax_loc_id', how = 'outer')
data = pd.merge(data, components, on = 'comp_id', how = 'outer')
data = pd.merge(data, products, left_on = 'comp_id', right_on = 'prod_id', how = 'outer')
data = pd.merge(data, states, left_on = 'cus_state', right_on = 'state_id', how = 'outer')



print(orders.head())
print(customers.head())
print(data.head())

CONN.close()