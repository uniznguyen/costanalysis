import pyodbc
import pandas as pd
import numpy as np



cn = pyodbc.connect('DSN=QuickBooks Data;')
sql = """SELECT * FROM ItemInventoryAssembly"""
df = pd.read_sql(sql,cn)


print (df.info())