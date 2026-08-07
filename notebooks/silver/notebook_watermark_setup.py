#!/usr/bin/env python
# coding: utf-8

# ## notebook_watermark_setup
# 
# null

# **Add Imports**
# 

# In[7]:


from pyspark.sql import Row
from delta.tables import DeltaTable
from pyspark.sql.functions import coalesce, col, to_date, when


# **Add a watermark table in Bronze**

# In[8]:


from pyspark.sql import Row

# Run once to initialize — creates the watermark table if it doesn't exist
watermark_table = "bronze.load_watermarks"

if not spark.catalog.tableExists(watermark_table):
    spark.createDataFrame(
        [Row(table_name="repayments", last_loaded_id="REP0000000")]
    ).write.format("delta").mode("overwrite").saveAsTable(watermark_table)
    print("Watermark table created.")
else:
    print("Watermark table already exists.")


# **Read the current watermark before copying**

# In[9]:


spark.sql("SELECT last_loaded_id FROM bronze.load_watermarks WHERE table_name = 'repayments'").show()


# **Incremental MERGE in the Silver notebook**

# In[16]:


# get last processed ID
watermark_df = spark.table("bronze.load_watermarks").filter("table_name = 'repayments'")
last_loaded_id = watermark_df.collect()[0]["last_loaded_id"]
print(f"Watermark: {last_loaded_id}")

# only new rows since last run
bronze_repayments = spark.table("bronze.repayments")
new_repayments = bronze_repayments.filter(f"RepaymentID > '{last_loaded_id}'")

new_count = new_repayments.count()
print(f"New rows: {new_count}")

if new_count > 0:

    # fix mixed date formats
    def parse_mixed_date(col_name: str):
        slash_format = to_date(col(col_name), "dd/MM/yyyy")
        iso_format = to_date(col(col_name), "yyyy-MM-dd")
        return coalesce(slash_format, iso_format)

    # clean new rows
    cleaned_new = (
        new_repayments
        .dropna(subset=["RepaymentID", "LoanID", "AmountDue", "Status"])
        .withColumn("DueDate", parse_mixed_date("DueDate"))
        .withColumn("PaidDate", parse_mixed_date("PaidDate"))
        .withColumn("AmountDue", col("AmountDue").cast("double"))
        .withColumn(
            "AmountPaid",
            when(col("AmountPaid") == "", None).otherwise(col("AmountPaid")).cast("double"),
        )
    )

    # upsert into Silver
    silver_table = DeltaTable.forName(spark, "silver.silver_repayments")
    (
        silver_table.alias("target")
        .merge(cleaned_new.alias("source"), "target.RepaymentID = source.RepaymentID")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Merged {new_count} rows")

    # move watermark forward
    new_max_id = new_repayments.agg({"RepaymentID": "max"}).collect()[0][0]
    spark.sql(f"""
        UPDATE bronze.load_watermarks
        SET last_loaded_id = '{new_max_id}'
        WHERE table_name = 'repayments'
    """)
    print(f"Watermark updated to {new_max_id}")

else:
    print("Nothing new, skipped merge.")


# In[14]:


spark.sql("DESCRIBE EXTENDED silver.silver_repayments").show(100, truncate=False)

