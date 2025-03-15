# Databricks notebook source
# MAGIC %md 
# MAGIC ###user Defined Function
# MAGIC

# COMMAND ----------

def my_func(x):
    return x*x

# COMMAND ----------

my_udf = udf(my_func)

# COMMAND ----------

df=spark.read.format('csv').option('Inferschema',True).option('Header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.withColumn('mynewcolumn',my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Writing
# MAGIC -->Append()
# MAGIC -->Overwrite()
# MAGIC -->Error()
# MAGIC -->Ignore()

# COMMAND ----------


