#!/usr/bin/env python
# coding: utf-8

# ## silver_cleaning
# 
# null

# **Load Bronze CSV Files**

# In[2]:


df_customers_raw = spark.read.option("header", "true").csv("Files/bronze/customers.csv")
df_loans_raw = spark.read.option("header", "true").csv("Files/bronze/loans.csv")
df_repayments_raw = spark.read.option("header", "true").csv("Files/bronze/repayments.csv")
df_defaults_raw = spark.read.option("header", "true").csv("Files/bronze/defaults.csv")

print("Bronze row counts:")
print("Customers:", df_customers_raw.count())
print("Loans:", df_loans_raw.count())
print("Repayments:", df_repayments_raw.count())
print("Defaults:", df_defaults_raw.count())


# **Converts dates from different formats into one standard format.**

# In[3]:


from pyspark.sql.functions import col, to_date, coalesce, when, upper,trim,row_number
from pyspark.sql.window import Window

def normalize_date(colname):
    """Try different date formats until one works, then return a clean date."""
    return coalesce(
        to_date(col(colname), "yyyy-MM-dd"),
        to_date(col(colname), "dd/MM/yyyy"),
        to_date(col(colname), "MM-dd-yyyy"),
        to_date(col(colname), "dd MMM yyyy"),
    )


# **Clean Customer Data**

# In[21]:


df_customers_clean = (
    df_customers_raw
     .withColumn("DOB_clean", normalize_date("DOB"))
     .withColumn("MonthlyIncome", col("MonthlyIncome").cast("double"))
     .withColumn("CreditScore", col("CreditScore").cast("int"))
     .withColumn("Province", trim(col("Province")))
)

window_spec = Window.partitionBy("CustomerID").orderBy(col("MonthlyIncome").desc_nulls_last())
df_customers_deduped = (
    df_customers_clean
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num", "DOB")
    .withColumnRenamed("DOB_clean", "DOB")
)

df_customers_final = (
    df_customers_deduped
     .fillna({"Province": "Unknown", "EmploymentStatus": "Unknown"})
     .withColumn("CreditScore", when(col("CreditScore").isNull(), 600).otherwise(col("CreditScore")))
)

print("Customers after deduplication:", df_customers_final.count())
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
df_customers_final.write.format("delta").mode("overwrite").saveAsTable("silver.silver_customers")


# **Clean Loans Data**

# In[13]:


df_loans_final = (
   df_loans_raw
   .withColumn("DisbursementDate", normalize_date("DisbursementDate"))
   .withColumn("Amount", col("Amount").cast("double"))
   .withColumn("InterestRate", col("InterestRate").cast("double"))
   .withColumn("TermMonths", col("TermMonths").cast("int"))
   .dropDuplicates(["LoanID"])
)
print("Loans after cleaning:", df_loans_final.count())
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
df_loans_final.write.format("delta").mode("overwrite").saveAsTable("silver.silver_loans")


# **Clean Repayments**

# In[17]:


df_repayments_final = (
    df_repayments_raw
    .withColumn("DueDate", normalize_date("DueDate"))
    .withColumn("PaidDate", to_date(col("PaidDate"), "yyyy-MM-dd"))  # already consistent format
    .withColumn("AmountDue", col("AmountDue").cast("double"))
    .withColumn("AmountPaid", col("AmountPaid").cast("double"))
    .withColumn("AmountPaid", when(col("AmountPaid").isNull(), 0.0).otherwise(col("AmountPaid")))
    .withColumn("Status", upper(trim(col("Status"))))
    .dropDuplicates(["RepaymentID"])
)

print("Repayments after cleaning:", df_repayments_final.count())
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
df_repayments_final.write.format("delta").mode("overwrite").saveAsTable("silver.silver_repayments")


# **Clean Defaults Data**

# In[19]:


df_defaults_final = (
    df_defaults_raw
    .withColumn("DefaultDate", normalize_date("DefaultDate"))
    .withColumn("DefaultFlag", upper(trim(col("DefaultFlag"))))
    .dropDuplicates(["LoanID"])
)
print("Defaults after cleaning:", df_defaults_final.count())
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
df_defaults_final.write.format("delta").mode("overwrite").saveAsTable("silver.silver_defualts")


# In[5]:


spark.sql("SHOW TABLES IN bronze").show(truncate=False)

