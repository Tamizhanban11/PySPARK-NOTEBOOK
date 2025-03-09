# Databricks notebook source
dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

df = spark.read.format('csv').option('Header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Alias

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col('Item_Type').alias('Item')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##FILTER
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario 2 using 2 filter Option

# COMMAND ----------

from pyspark.sql.functions import col
df.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Scenario 4
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###withColumnRename()

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_WT').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###WithColumn()

# COMMAND ----------

from pyspark.sql.functions import lit
df=df.withColumn('Flag',lit("new")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### multiply the two column values
# MAGIC

# COMMAND ----------

df = spark.read.format('csv').option('Header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

from pyspark.sql.functions import col
df.withColumn('multiply',col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

print(df)
