# Databricks notebook source
dbutils.fs.ls('/FileStore/')

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df = spark.read.format('csv').option('Header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Changing thevalue within the column

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","Reg")).display()

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Low Fat","LF")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##CastTyping()

# COMMAND ----------

from pyspark.sql.types import IntegerType
df = df.withColumn('Item_Weight',col('Item_Weight').cast(IntegerType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sort()

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC  Sorting Done for Two Columns
# MAGIC

# COMMAND ----------

df.sort('Item_MRP','Item_Visibility',asc = [1,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##different sorting in multipke column

# COMMAND ----------

df.sort('Item_Weight','Item_MRP',asc=[0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Limit()

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##
# MAGIC Drop()

# COMMAND ----------

df.drop('Outlet_Size').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dropping Multiple Column

# COMMAND ----------

df.drop('Item_Visibility','Item_Weight').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DropDuplicates()

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop_Duplicates()

# COMMAND ----------

df.drop_duplicates(subset = ['Item_Type']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Distinct()
# MAGIC

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating Data Frames
# MAGIC

# COMMAND ----------

data1 = [('1','Tamil'),('2','hari')]
schema1 = 'id String','id String'
df1 = spark.createDataFrame(data1,schema1)


# COMMAND ----------

data2 = [('3','bala'),('4','Siva')]
schema2 = 'id String','id String'
df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Union()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###UnionByName()

# COMMAND ----------

data2 = [('hello','1'),('wold','2')]
schema2 = 'name String ', '  id String'
df2 = spark.createDataFrame(data2, schema2)

# COMMAND ----------

df2.display()

# COMMAND ----------

data3 = [('3','bala'),('4','Siva')]
schema3 = 'id String', ' name String'
df3 = spark.createDataFrame(data3,schema3)

# COMMAND ----------

df3.display()

# COMMAND ----------

df2.union(df3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###String Functions initCap()

# COMMAND ----------

from pyspark.sql.functions import initcap
df.select(initcap('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Lowercase()

# COMMAND ----------

from pyspark.sql.functions import lower
df.select(lower('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Upper()

# COMMAND ----------

from pyspark.sql.functions import upper
df.select(upper('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Date Functions()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Current_date()

# COMMAND ----------

from pyspark.sql.functions import current_date
df = df.withColumn('curr_date',current_date()).display()
