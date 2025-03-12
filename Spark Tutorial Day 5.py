# Databricks notebook source
dbutils.fs.ls('FileStore/tables')

# COMMAND ----------

df = spark.read.format('csv').option('Inferschema',True).option('Header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Split() function

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import split
df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###indexing()

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type','')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Explode()

# COMMAND ----------



# COMMAND ----------

df = spark.read.format('csv').option('Inferschema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')
df.withColumn('Outlet_Type',split('Outlet_Type','')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ####array_contains()

# COMMAND ----------

from pyspark.sql.functions import array_contains
df.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ####array_contains()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Groupby()

# COMMAND ----------

from pyspark.sql.functions import sum
df.groupBy('Item_type').agg(sum('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Avg()

# COMMAND ----------

from pyspark.sql.functions import avg
df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### multiple grouping()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_size').agg(sum('Item_MRP').alias('Total_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_size').agg(sum('Item_MRP'),avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Collect_List()

# COMMAND ----------

data = [('user1','book1'),('user2','book1'),('user2','book2'),('user1','book2')]
schema = 'user','book'
df_book = spark.createDataFrame(data,schema)
df_book.display()

# COMMAND ----------

from pyspark.sql.functions import collect_list
df_book.groupBy('user').agg(collect_list('book')).display()
