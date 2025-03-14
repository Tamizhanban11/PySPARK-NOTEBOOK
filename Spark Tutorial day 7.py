# Databricks notebook source
dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

df = spark.read.format('csv').option('Inferschema',True).option('Header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create DataFrame

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)



# COMMAND ----------

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Joins()

# COMMAND ----------

# MAGIC %md
# MAGIC #####INNER JOIN()

# COMMAND ----------


df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####LEFT JOIN()

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####RIGHT JOIN()

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###LEFT JOIN

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####ANTI ()

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Window Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ####Row_number()

# COMMAND ----------

from pyspark.sql.window import  Window
from pyspark.sql.functions import row_number
df.withColumn('rolCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###RANK()

# COMMAND ----------

from pyspark.sql.functions import rank
df.withColumn('rank',rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####CUM SUM()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum
df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()
