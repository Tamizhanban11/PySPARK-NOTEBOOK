# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Reading Json And CSV
# MAGIC

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('Header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df1 = spark.read.format('json').option('inferschema',True).option('Header',True).load('/FileStore/tables/drivers.json')

# COMMAND ----------

df1.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema = ('''  Item_Identifier INT , Item_Weight STRING, Item_Fat_Content STRING, Item_Visibility DOUBLE,Item_Type STRING,Item_MRP DOUBLE, Outlet_Identifier STRING, Outlet_Establishment_Year INT,  Outlet_Size STRING,Outlet_Location_Type STRING, Outlet_Type STRING, Item_Outlet_Sales DOUBLE ''')

# COMMAND ----------

df= spark.read.format('csv')\
    .schema(my_ddl_schema)\
        .option('header',True)\
            .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## structType()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema = StructType([StructField('Item_Identifier',StringType(),True)])

# COMMAND ----------

df = spark.read.format('csv')\
    .schema(my_struct_schema)\
        .option('Header',True)\
            .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## select() 
# MAGIC
# MAGIC

# COMMAND ----------

df1.display()

# COMMAND ----------

df_sel = df1.select('code','dob','driverId').display()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df1.select(col('code'),col('dob'),col('driverId')).display()
