import pyodbc
import pandas as pd
import numpy as np
import sqlite3 as db
import os

ITEMS = """'3-FG:82592','3-FG:85592','3-FG:82594','3-FG:83594','3-FG:85594','3-FG:81596','3-FG:82596','3-FG:83596',
'3-FG:85596','3-FG:81595','3-FG:82595','3-FG:83595'"""

cn = pyodbc.connect('DSN=QuickBooks Data;')
sql = """SELECT ListID, Name, FullName, IsActive, ParentRefFullName, SalesDesc,PurchaseDesc, PurchaseCost, AverageCost 
FROM ItemInventoryAssembly"""
sql2 = """SELECT ListID, Name, FullName, IsActive, ParentRefFullName, SalesDesc,PurchaseDesc, PurchaseCost, AverageCost 
FROM ItemInventory"""
sql3 = """SELECT
	ListID,
	Name,
	FullName,
	IsActive,
	ParentRefFullName,
	SalesDesc,
	PurchaseDesc,
	PurchaseCost,
	AverageCost,
	ItemInventoryAssemblyLnItemInventoryRefFullName,
	ItemInventoryAssemblyLnQuantity
FROM
	ItemInventoryAssemblyLine"""

sql11 = f"""SELECT Name, PriceLevelPerItemItemRefFullName, PriceLevelPerItemCustomPrice
FROM PriceLevelPerItem
WHERE Name = '2018 HV Mid Yr' AND PriceLevelPerItemItemRefFullName IN ({ITEMS})
"""

df_item_assembly = pd.read_sql(sql,cn)
df_item_part = pd.read_sql(sql2,cn)
df_item_assembly_line = pd.read_sql(sql3,cn)
df_PriceLevelPerItem = pd.read_sql(sql11,cn)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_filename = os.path.join(BASE_DIR,'sqllite.db')
con = db.connect(db_filename)

df_item_assembly.to_sql('ItemInventoryAssembly',con, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None)
df_item_part.to_sql('ItemInventory',con, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None)
df_item_assembly_line.to_sql('ItemInventoryAssemblyLine',con, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None)
df_PriceLevelPerItem.to_sql('PriceLevelPerItem',con, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None)

sql4 = f"""WITH RECURSIVE RPL (FullName, ItemInventoryAssemblyLnItemInventoryRefFullName, ItemInventoryAssemblyLnQuantity) AS
    (
       SELECT ROOT.FullName, Root.ItemInventoryAssemblyLnItemInventoryRefFullName, Root.ItemInventoryAssemblyLnQuantity
       FROM ItemInventoryAssemblyLine ROOT
       WHERE ROOT.FullName IN ({ITEMS})
       UNION ALL
      SELECT PARENT.FULLNAME, CHILD.ItemInventoryAssemblyLnItemInventoryRefFullName, PARENT.ItemInventoryAssemblyLnQuantity*CHILD.ItemInventoryAssemblyLnQuantity
    FROM RPL PARENT, ItemInventoryAssemblyLine CHILD
    WHERE PARENT.ItemInventoryAssemblyLnItemInventoryRefFullName = CHILD.Fullname            
    )

    
    SELECT RPL.FullName, RPL.ItemInventoryAssemblyLnItemInventoryRefFullName,SUM(RPL.ItemInventoryAssemblyLnQuantity) AS "TotalQTYUsed"
    FROM RPL
    GROUP BY RPL.FullName, RPL.ItemInventoryAssemblyLnItemInventoryRefFullName
    ORDER BY RPL.FullName, RPL.ItemInventoryAssemblyLnItemInventoryRefFullName"""

cursor = con.cursor()
# cursor.execute(sql4)

df_recursive = pd.read_sql(sql4,con)

#df_recursive = pd.DataFrame(cursor.fetchall(),columns=['FullName','ItemInventoryAssemblyLnItemInventoryRefFullName','TotalQTYUsed'])

df_recursive.to_sql('Recursive',con,schema=None,if_exists='replace', index=True, index_label=None, chunksize=None)

# df_merge = pd.merge(df_recursive, df_item_part, left_on='ItemInventoryAssemblyLnItemInventoryRefFullName', right_on='FullName', how='inner', indicator = True)


# print (df_merge.head())
# print (df_merge.info())

# df_merge['cost'] = df_merge[['PurchaseCost','AverageCost']].max(axis=1)
# df_merge.to_sql('Merge',con,schema=None,if_exists='replace', index=True, index_label=None, chunksize=None)

sql7 = """DROP TABLE IF EXISTS Merge"""
cursor.execute(sql7)

sql8 = """CREATE TABLE Merge  AS SELECT C.FullName, C.ItemInventoryAssemblyLnItemInventoryRefFullName, C.TotalQTYUsed, Max(I.PurchaseCost, I.AverageCost) as CostPerUnit, Max(I.PurchaseCost, I.AverageCost) * C.TotalQTYUsed As TotalCost
FROM Recursive C INNER JOIN ItemInventory I ON C.ItemInventoryAssemblyLnItemInventoryRefFullName = I.FullName ORDER BY C.FullName"""
cursor.execute(sql8)


sql5 = """SELECT C.FullName, C.ItemInventoryAssemblyLnItemInventoryRefFullName, C.TotalQTYUsed, Max(I.PurchaseCost, I.AverageCost) as CostPerUnit, Max(I.PurchaseCost, I.AverageCost) * C.TotalQTYUsed
FROM Recursive C INNER JOIN ItemInventory I ON C.ItemInventoryAssemblyLnItemInventoryRefFullName = I.FullName"""
cursor.execute(sql5)
df5 = pd.DataFrame(cursor.fetchall(), columns = ['FullName', 'ItemInventoryAssemblyLnItemInventoryRefFullName', 'C.TotalQTYUsed','CostPerUnit','TotalCost'])

sql6 = """SELECT C.FullName, C.ItemInventoryAssemblyLnItemInventoryRefFullName, C.TotalQTYUsed, Max(IA.PurchaseCost, IA.AverageCost) as CostPerUnit, Max(IA.PurchaseCost, IA.AverageCost) * C.TotalQTYUsed
FROM Recursive C INNER JOIN ItemInventoryAssembly IA ON C.ItemInventoryAssemblyLnItemInventoryRefFullName = IA.FullName"""
cursor.execute(sql6)
df6 = pd.DataFrame(cursor.fetchall(), columns = ['FullName', 'ItemInventoryAssemblyLnItemInventoryRefFullName', 'C.TotalQTYUsed','CostPerUnit','TotalCost'])

df_total_cost = pd.concat([df5,df6])

df_total_cost.to_sql('TotalCost',con,schema=None,if_exists='replace', index=True, index_label=None, chunksize=None)

sql9 = """DROP TABLE IF EXISTS CompareCost"""
cursor.execute(sql9)
sql9 = """CREATE TABLE IF NOT EXISTS CompareCost AS 
SELECT M.FullName, IA.PurchaseCost, Sum(M.TotalCost) As ActualCost, PI.PriceLevelPerItemCustomPrice AS HVPrice
FROM Merge M INNER JOIN ItemInventoryAssembly IA ON M.FullName = IA.FullName
INNER JOIN PriceLevelPerItem PI ON M.FullName = PI.PriceLevelPerItemItemRefFullName
GROUP BY M.FullName"""

cursor.execute(sql9)

cn.close()
cursor.close()
con.close()