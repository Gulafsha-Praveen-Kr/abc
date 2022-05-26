# Databricks notebook source
import MySQLdb as db_connect

host_name="localhost"
db_user="test_user"
db_password="123"
db_name="project

connection = db_connect.connect(host=host_name,user=db_user,password=db_password,database=db_name)
 
cursor = connection.cursor()

query = "select * from data limit 5"

results = cursor.execute(query).fetchall()
print(result)

connection.close()

# COMMAND ----------

# MAGIC %sql
# MAGIC select key_vlu_cd,GNC_TX from refined_db20_tec.teccodes where (tbl_nam_id = '0344' and tbl_own_id='000') and SUBSTR (GNC_TX,16,2)   in ('FE') order by key_vlu_cd;

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions
import concat, col, lit,trim
val result = spark.sql((select key_vlu_cd,GNC_TX from refined_db20_tec.teccodes where (tbl_nam_id = '0344' and tbl_own_id='000') and SUBSTR (GNC_TX,16,2)   in (select trim(key_vlu_cd) from refined_db20_tec.teccodes  where tbl_nam_id = '0296' and tbl_own_id='000') order by key_vlu_cd));
 print(Result)                      



# COMMAND ----------

import pandas as pd
df = spark.sql("select key_vlu_cd,GNC_TX from refined_db20_tec.teccodes")
print(df)

# COMMAND ----------

