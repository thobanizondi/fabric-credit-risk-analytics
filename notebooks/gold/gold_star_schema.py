#!/usr/bin/env python
# coding: utf-8

# ## gold_star_schema
# 
# null

# **Load Silver Tables**

# In[2]:


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

# In[ ]:


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

# In[5]:


dim_loantype = (
    df_loans.select("LoanType").distinct()
    .withColumn("LoanTypeID", monotonically_increasing_id())
    .select("LoanTypeID", "LoanType")
)
dim_loantype.write.format("delta").mode("overwrite").saveAsTable("gold.dim_loantype")
dim_loantype.show()


# **Create dim_Date**

# In[7]:


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

# In[9]:


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

# In[11]:


fact_repayments = (
    df_repayments
    .withColumn("DueDateID", date_format(col("DueDate"), "yyyyMMdd").cast("int"))
    .withColumn("DaysLate",
         when(col("Status") == "LATE", datediff(col("PaidDate"), col("DueDate")))
         .otherwise(0)
    )
    .select(
        "RepaymentID",
        "LoanID",
        "DueDateID",
        "AmountDue",
        "AmountPaid",
        "Status",
        "DaysLate"
    )
)
fact_repayments.write.format("delta").mode("overwrite").saveAsTable("gold.fact_repayments")
print("gold.fact_repayments rows:", fact_repayments.count())


# **Validation Query (Real Insight)**

# In[12]:


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

