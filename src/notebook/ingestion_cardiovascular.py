# Databricks notebook source
display(dbutils.fs)

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

dbutils.fs.mkdirs("/tmp/")

# COMMAND ----------

display(dbutils.fs.ls("/tmp/"))

# COMMAND ----------

display(dbutils.fs.ls("/tmp/"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Extraindo dados/Realizando a leitura

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load("/tmp/cardiovascular.csv/")

# COMMAND ----------

df.display()

# COMMAND ----------

df.show()

# COMMAND ----------

df.select("General_Health").distinct().display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Realizando o armazenamento de dados

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("General_Health").save("/hospital/rw/cardiovascular")

# COMMAND ----------

# MAGIC %md
# MAGIC #Rename cols

# COMMAND ----------

df = df.withColumnRenamed("Height_(cm)", "Height_cm").withColumnRenamed("Weight_(kg)", "Weight_kg")

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("General_Health").save("/hospital/rw/cardiovascular")

# COMMAND ----------

# MAGIC %md
# MAGIC #Criando database e tabela pelo delta location

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS db_hospital

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS db_hospital.cardiovascular_diseases
# MAGIC LOCATION "/hospital/rw/cardiovascular/"
# MAGIC

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("General_Health").saveAsTable("db_hospital.rw_cardiovascular_diseases")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM db_hospital.rw_cardiovascular_diseases
