#!/usr/bin/env python
# coding: utf-8

# ## notebook_watermark_setup
# 
# null

# **Add Imports**
# 

# In[2]:


from pyspark.sql import Row
from delta.tables import DeltaTable
from pyspark.sql.functions import coalesce, col, to_date, when


# In[3]:


# creates watermark rows if missing — safe to rerun daily
watermark_table = "bronze.load_watermarks"

if not spark.catalog.tableExists(watermark_table):
    spark.createDataFrame([
        Row(table_name="repayments", last_loaded_id="REP0000000"),
        Row(table_name="repayments_gold", last_loaded_id="REP0000000"),
    ]).write.format("delta").mode("overwrite").saveAsTable(watermark_table)
    print("Watermark table created.")
else:
    existing_rows = [
        r["table_name"] for r in spark.table(watermark_table).select("table_name").collect()
    ]

    # only add rows that don't exist yet
    rows_to_add = []
    if "repayments" not in existing_rows:
        rows_to_add.append(Row(table_name="repayments", last_loaded_id="REP0000000"))
    if "repayments_gold" not in existing_rows:
        rows_to_add.append(Row(table_name="repayments_gold", last_loaded_id="REP0000000"))

    if rows_to_add:
        spark.createDataFrame(rows_to_add).write.format("delta").mode("append").saveAsTable(watermark_table)
        print(f"Added: {[r['table_name'] for r in rows_to_add]}")
    else:
        print("Watermarks already exist, nothing to do.")

