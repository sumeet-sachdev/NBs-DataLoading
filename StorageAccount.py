# Databricks notebook source
storage_account = 'sumeetssa02augv1'
container_name = 'v1container'
source_url = f'wasbs://{container_name}@{storage_account}.blob.core.windows.net'

access_key = ''
mount_point_url = '/mnt/'

extra_configs_key = f'fs.azure.account.key.{storage_account}.blob.core.windows.net'
extra_configs_value = access_key
extra_configs_dict = {extra_configs_key:extra_configs_value}

# COMMAND ----------

dbutils.fs.mount(source = source_url,
                 mount_point = mount_point_url,
                 extra_configs = {
                     extra_configs_key: extra_configs_value
                 })

# COMMAND ----------

dbutils.fs.ls(mount_point_url)

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

extra_configs_key = f'fs.azure.account.key.{storage_account}.blob.core.windows.net'
extra_configs_value = dbutils.secrets.get(
    scope= "",
    key= "sav1"
)
extra_configs_dict = {extra_configs_key:extra_configs_value}

# COMMAND ----------


