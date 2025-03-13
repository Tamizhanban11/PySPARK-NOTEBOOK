# Databricks notebook source
dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

df = spark.read.format('csv').option('Inferschema',True).option('Header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Pivot()Function

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import avg
df.groupby('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####when-otherwise()

# COMMAND ----------

from pyspark.sql.functions import col, when
df.withColumn('veg_flag',when(col('Item_Type')=='Meat','Non Veg').otherwise('Veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###using multile condition

# COMMAND ----------

from pyspark.sql.functions import col, when
df = df.withColumn('veg_flag', when(col('Item_Type') == 'Meat', 'Non Veg').otherwise('Veg'))
df = df.withColumn(
    'veg_Exp',
    when((col('veg_flag') == 'Veg') & (col('Item_MRP') > 200), 'Inexpensive')
    .when((col('veg_flag') == 'Veg') & (col('Item_MRP') < 100), 'OutExpensive')
    .otherwise('NonVeg')
)
df.show()

