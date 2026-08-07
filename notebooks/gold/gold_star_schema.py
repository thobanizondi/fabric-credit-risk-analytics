#!/usr/bin/env python
# coding: utf-8

# ## gold_star_schema
# 
# null

# **Load Silver Tables**

# In[1]:


from pyspark.sql.functions import (
    col, when, monotonically_increasing_id, explode, sequence,
    to_date, year, month, dayofmonth,
    date_format, datediff, floor, current_date
)

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

df_customers = spark.table("silver.silver_customers")
df_loans = spark.table("silver.silver_loans")
df_repayments = spark.table("silver.silver_repayments")
df_defaults = spark.table("silver.silver_defualts")

print("Silver row counts")
print("Customers:", df_customers.count())
print("Loans:", df_loans.count())
print("Repayments:", df_repayments.count())
print("Defaults:", df_defaults.count())


# **Create dim_Customers**

# In[2]:


dim_customers = (
    df_customers
    .withColumn("Age", floor(datediff(current_date(), col("DOB")) / 365.25))
    .withColumn(
        "CreditScoreBand",
        when(col("CreditScore") < 400, "300-399")
        .when(col("CreditScore") < 500, "400-499")
        .when(col("CreditScore") < 600, "500-599")
        .when(col("CreditScore") < 700, "600-699")
        .when(col("CreditScore") < 800, "700-799")
        .otherwise("800-850")
    )
    .select(
        "CustomerID", "FirstName", "LastName", "DOB", "Age",
        "Province", "MonthlyIncome", "EmploymentStatus", "CreditScore", "CreditScoreBand"
    )
)

dim_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dim_customers")
print("gold.dim_customers rows:", dim_customers.count())


# **Create dim_LoanType**

# In[3]:


dim_loantype = (
    df_loans.select("LoanType").distinct()
    .withColumn("LoanTypeID", monotonically_increasing_id())
    .select("LoanTypeID", "LoanType")
)
dim_loantype.write.format("delta").mode("overwrite").saveAsTable("gold.dim_loantype")
dim_loantype.show()


# **Create dim_Date**

# In[4]:


date_range = spark.sql("""
    SELECT explode(sequence(to_date('2020-01-01'), to_date('2028-12-31'), interval 1 day)) as FullDate
""")
dim_date = (
    date_range
    .withColumn("DateID", date_format(col("FullDate"), "yyyyMMdd").cast("int"))
    .withColumn("Year", year(col("FullDate")))
    .withColumn("Month", month(col("FullDate")))
    .withColumn("Day", dayofmonth(col("FullDate")))
    .withColumn("MonthName", date_format(col("FullDate"), "MMMM"))
)
dim_date.write.format("delta").mode("overwrite").saveAsTable("gold.dim_date")
print("gold.dim_date rows:", dim_date.count())


# **Create fact_Loans**

# In[5]:


fact_loans = (
    df_loans
        .join(dim_loantype, "LoanType", "left")
        .join(df_defaults, "LoanID", "left")
        .join(
            df_customers.select("CustomerID", "CreditScore"),
            "CustomerID",
            "left"
        )
        .withColumn(
            "RiskScore",
            when(col("CreditScore") >= 700, "Low")
            .when(col("CreditScore") >= 600, "Medium")
            .otherwise("High")
        )
        .withColumn(
            "DisbursementDateID",
            date_format(col("DisbursementDate"), "yyyyMMdd").cast("int")
        )
        .select(
            "LoanID",
            "CustomerID",
            "LoanTypeID",
            "Amount",
            "InterestRate",
            "TermMonths",
            "DisbursementDateID",
            "RiskScore",
            when(col("DefaultFlag") == "YES", 1)
            .otherwise(0)
            .alias("IsDefault")
        )
)


fact_loans.write.format("delta").mode("overwrite").saveAsTable("gold.fact_loans")
print("gold.fact_loans rows:", fact_loans.count())


# **Create fact_Repayments**

# In[20]:


from delta.tables import DeltaTable

# get Gold's own watermark (separate from Silver's)
gold_watermark_df = spark.table("bronze.load_watermarks").filter("table_name = 'repayments_gold'")
gold_last_id = gold_watermark_df.collect()[0]["last_loaded_id"]
print(f"Gold watermark: {gold_last_id}")

# only rows Silver has that Gold hasn't processed yet
new_silver_repayments = df_repayments.filter(f"RepaymentID > '{gold_last_id}'")
new_count = new_silver_repayments.count()
print(f"New rows for Gold: {new_count}")

if new_count > 0:

    # same transform as before, applied only to the new slice
    new_fact_repayments = (
        new_silver_repayments
        .withColumn("DueDateID", date_format(col("DueDate"), "yyyyMMdd").cast("int"))
        .withColumn("DaysLate",
             when(col("Status") == "LATE", datediff(col("PaidDate"), col("DueDate")))
             .otherwise(0)
        )
        .select(
            "RepaymentID", "LoanID", "DueDateID",
            "AmountDue", "AmountPaid", "Status", "DaysLate"
        )
    )

    # first run ever: table doesn't exist yet, just create it
    if not spark.catalog.tableExists("gold.fact_repayments"):
        new_fact_repayments.write.format("delta").mode("overwrite").saveAsTable("gold.fact_repayments")
        print(f"gold.fact_repayments created with {new_count} rows")
    else:
        # normal case: upsert only the new rows
        fact_table = DeltaTable.forName(spark, "gold.fact_repayments")
        (
            fact_table.alias("target")
            .merge(new_fact_repayments.alias("source"), "target.RepaymentID = source.RepaymentID")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Merged {new_count} rows into gold.fact_repayments")

    # move Gold's watermark forward
    new_max_id = new_silver_repayments.agg({"RepaymentID": "max"}).collect()[0][0]
    spark.sql(f"""
        UPDATE bronze.load_watermarks
        SET last_loaded_id = '{new_max_id}'
        WHERE table_name = 'repayments_gold'
    """)
    print(f"Gold watermark updated to {new_max_id}")

else:
    print("Nothing new for Gold, skipped merge.")


# **Validation Query (Real Insight)**

# In[7]:


spark.sql("""
    SELECT
          c.Province,
          COUNT(DISTINCT f.LoanID) AS TotalLoans,
          SUM(f.IsDefault) as TotalDefaults,
          ROUND(SUM(f.IsDefault) * 100.0 / COUNT(DISTINCT f.LoanID), 1) AS DefaultRatePct
    FROM gold.fact_loans f
    JOIN gold.dim_customers c ON f.CustomerID = c.CustomerID
    GROUP BY c.Province
    ORDER BY DefaultRatePct DESC
""").show()


# In[8]:


from pyspark.sql.types import StructType, StructField, StringType, FloatType

schema = StructType([
    StructField("Province", StringType(), True),
    StructField("MaxAcceptableDefaultRate", FloatType(), True),
    StructField("ReviewPriority", StringType(), True)
])

data = [
    ("Gauteng", 8.0, "Standard"),
    ("Western Cape", 7.0, "Standard"),
    ("KwaZulu-Natal", 9.0, "Standard"),
    ("Eastern Cape", 10.0, "Elevated"),
    ("Free State", 10.0, "Elevated"),
    ("Limpopo", 11.0, "Elevated"),
    ("Mpumalanga", 10.0, "Standard"),
    ("North West", 11.0, "Elevated"),
    ("Northern Cape", 12.0, "Elevated"),
    ("Unknown", 8.0, "Standard"),
]

df_thresholds = spark.createDataFrame(data, schema)
df_thresholds.write.format("delta").mode("overwrite").saveAsTable("gold.risk_thresholds")
print("gold.risk_thresholds created:", df_thresholds.count(), "rows")


# **View table history**

# In[9]:


display(spark.sql("DESCRIBE HISTORY gold.fact_loans"))


# **Creating version 2 of the fact_loans table**

# In[10]:


from pyspark.sql.functions import lit
df_fact_loans = spark.table ("gold.fact_loans")

df_fact_loans_v2 = df_fact_loans.withColumn("DataVersion", lit("v2_test"))

df_fact_loans_v2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_loans")

print("New version created.")


# **Checking history**

# In[11]:


display(spark.sql("DESCRIBE HISTORY gold.fact_loans"))


# **Time travel: query version 0**

# In[12]:


df_v0 = spark.sql("SELECT * FROM gold.fact_loans VERSION AS OF 0")
print("Version 0 columns:", df_v0.columns)
print("Version 0 row count:", df_v0.count())


# **Query the current version (version 2)**

# In[13]:


df_current = spark.sql ("SELECT * FROM gold.fact_loans")
print("Version 0 columns:", df_current.columns)
print("Version 0 row count:",df_current.count())


# In[14]:


df_clean = spark.table("gold.fact_loans").drop("DataVersion")
df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.fact_loans")
print("Cleaned up - DataVersion column removed.")


# In[15]:


spark.sql("OPTIMIZE gold.fact_loans")


# In[16]:


spark.sql("VACUUM gold.fact_loans RETAIN 168 HOURS")


# **Add watermark row for Gold**

# In[17]:


# add a Gold-specific watermark, separate from Silver's
spark.sql("""
    INSERT INTO bronze.load_watermarks (table_name, last_loaded_id)
    VALUES ('repayments_gold', 'REP0000000')
""")


# In[18]:


spark.sql("SELECT * FROM bronze.load_watermarks").show()

