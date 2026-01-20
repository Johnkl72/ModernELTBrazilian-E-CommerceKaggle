# Databricks notebook source
import os, textwrap, subprocess
from datetime import datetime

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date") or datetime.now().strftime("%Y-%m-%d")

base = "/Volumes/workspace/default/data/olist"
landing_run = f"{base}/landing/{run_date}"
dbutils.fs.mkdirs(landing_run)

kaggle_json = "/Volumes/workspace/default/data/secrets/kaggle.json"
dataset = "olistbr/brazilian-ecommerce"

print("Landing run:", landing_run)

# COMMAND ----------

customers_file = f"{landing_run}/olist_customers_dataset.csv"
exists = len(dbutils.fs.ls(landing_run)) > 0

dbutils.widgets.dropdown("force_download", "false", ["true","false"])
force = dbutils.widgets.get("force_download") == "true"

if exists and not force:
    print("Landing ya existe, no descargo.")
else:
    cmd = f"""
    set -e
    mkdir -p "{landing_run}"
    mkdir -p ~/.kaggle
    cp "{kaggle_json}" ~/.kaggle/kaggle.json
    chmod 600 ~/.kaggle/kaggle.json
    pip -q install kaggle
    kaggle datasets download -d {dataset} -p "{landing_run}" --unzip
    ls -lah "{landing_run}"
    """
    subprocess.run(["bash","-lc",cmd], check=True)
