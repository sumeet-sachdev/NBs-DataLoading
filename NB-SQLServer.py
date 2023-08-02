# Databricks notebook source
# MAGIC %python
# MAGIC print('Hello World from Python!')

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd

# COMMAND ----------

df_remote_table = (spark.read
                   .format('sqlserver')
                   .option('host', "sumeets-db-server.database.windows.net")
                   .option('port', "1433")
                   .option('user', "sumeets")
                   .option('password', "admin@123")
                   .option('database', "sumeets-db")
                   .option('dbtable', "dbo.iris_data")
                   .load()
                   )

# COMMAND ----------

df_remote_table.show()

# COMMAND ----------


