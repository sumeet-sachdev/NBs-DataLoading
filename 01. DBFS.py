# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.put('/tmp/abc.txt', 'Welcome to Databricks File System', True)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

access_key = dbutils.secrets.get(scope='aws', key='aws-access-key')
secret_key = dbutils.secrets.get(scope='aws', key='aws-secret-key')

# COMMAND ----------

#Mount bucket on databricks
encoded_secret_key = secret_key.replace("/", "%2F")

aws_bucket_name = "bkt-sumeets-adb"
mount_name = "sumeet-s3"

dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)

display(dbutils.fs.ls("/mnt/%s" % mount_name))


# COMMAND ----------

file_name="csv/iris.csv"
df = spark.read.format("csv").load("/mnt/%s/%s" % (mount_name , file_name))
df.show()

# COMMAND ----------

dbutils.fs.ls(f'/mnt/{mount_name}')

# COMMAND ----------

file_name="abc.parquet"
df = spark.read.format("parquet").load("/mnt/%s/%s" % (mount_name , file_name))
df.show()

# COMMAND ----------

# dbutils.fs.ls('s3://bkt-sumeets-adb/csv/iris.csv')

# COMMAND ----------

# spark.read.format('csv').load('s3://bkt-sumeets-adb/csv/iris.csv')

# COMMAND ----------

# spark.sql("SELECT * FROM csv.`'s3://bkt-sumeets-adb/csv/iris.csv'`")

# COMMAND ----------

str_mount_point = "/mnt/%s/%s" % (mount_name, file_name)

dbutils.fs.put(str_mount_point+'/abc.txt', 'Welcome to the File System')

# COMMAND ----------

str_mount_point_parquet = "/mnt/%s/abc.parquet" % (mount_name)

df.write.parquet(str_mount_point_parquet)


# COMMAND ----------


