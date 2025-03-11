# Databricks notebook source
dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

df = spark.read.format('csv').option('InferSchema',True).option('Header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##CurrDate()

# COMMAND ----------

from pyspark.sql.functions import current_date
df = df.withColumn('curr_date',current_date()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###HandlingNull

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####any()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####dropping null in particular column()

# COMMAND ----------

df.dropna(subset=['Item_Weight']).display()

# COMMAND ----------

df.fillna('NotAvailable').display()

# COMMAND ----------

df.fillna('NotAvailable').display()

# COMMAND ----------

df.fillna('NotAvailable',subset = ['Item_Weight']).display()
