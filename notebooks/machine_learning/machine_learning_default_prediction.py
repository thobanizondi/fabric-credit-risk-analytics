#!/usr/bin/env python
# coding: utf-8

# ## machine_learning_default_prediction
# 
# null

# **Imports functions, feature,classification, evaluation**
# 

# In[2]:


from pyspark.sql.functions import col, when
from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.functions import vector_to_array
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator


# **Load and join Gold tables into a single feature set**

# In[3]:


df_loans = spark.table("gold.fact_loans")
df_customers = spark.table("gold.dim_customers")
df_loantype = spark.table("gold.dim_loantype")

df_features = (
    df_loans
    .join (df_customers, "CustomerID", "left")
    .join (df_loantype, "LoanTypeID", "left")
    .select(
        "LoanID",
        "Amount",
        "InterestRate",
        "TermMonths",
        "CreditScore",
        "Age",
        "MonthlyIncome",
        "LoanType",
        "IsDefault"
    )
    .na.drop()
)
print("Feature dataset row count:", df_features.count())
df_features.show(5)


# **Encode categorical feature (LoanType) and assemble features**

# In[4]:


loan_type_indexer = StringIndexer(inputCol="LoanType", outputCol="LoanTypeIndex")

feature_cols = ["Amount", "InterestRate", "TermMonths", "CreditScore", "Age", "MonthlyIncome", "LoanTypeIndex"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")


# **Train/test split**

# In[5]:


train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)

# Add a weight column so the model penalizes missing a default much more heavily
train_df = train_df.withColumn(
    "classWeight",
    when(col("IsDefault") == 1, 10.0).otherwise(1.0)
)

print("Training rows:", train_df.count())
print("Test rows:", test_df.count())


# **Build and train the pipeline**

# In[6]:


lr = LogisticRegression(
    featuresCol="features",
    labelCol="IsDefault",
    weightCol="classWeight",
    maxIter=50
)

pipeline = Pipeline(stages=[loan_type_indexer, assembler, lr])
model = pipeline.fit(train_df)

print("Model trained successfully.")


# **Evaluate on test data**

# In[7]:


predictions = model.transform(test_df)

evaluator_auc = BinaryClassificationEvaluator(labelCol="IsDefault", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
auc = evaluator_auc.evaluate(predictions)

evaluator_acc = MulticlassClassificationEvaluator(labelCol="IsDefault", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator_acc.evaluate(predictions)

print(f"AUC-ROC: {auc:.4f}")
print(f"Accuracy: {accuracy:.4f}")

predictions.groupBy("IsDefault", "prediction").count().show()


# **Feature importance (coefficients)**

# In[8]:


lr_model = model.stages[-1]
coefficients = lr_model.coefficients.toArray()

for feature, coef in zip(feature_cols, coefficients):
    print(f"{feature}: {coef:.4f}")


# **Save predictions back to Gold layer**

# In[9]:


predictions_to_save = (
    predictions
    .select(
        "LoanID",
        "IsDefault",
        col("prediction").cast("int").alias("PredictedDefault"),
        vector_to_array(col("probability"))[1].alias("DefaultProbability")
    )
    .withColumn(
        "ProbabilityBand",
        when(col("DefaultProbability") < 0.1, "0.0-0.1")
        .when(col("DefaultProbability") < 0.2, "0.1-0.2")
        .when(col("DefaultProbability") < 0.3, "0.2-0.3")
        .when(col("DefaultProbability") < 0.4, "0.3-0.4")
        .when(col("DefaultProbability") < 0.5, "0.4-0.5")
        .when(col("DefaultProbability") < 0.6, "0.5-0.6")
        .when(col("DefaultProbability") < 0.7, "0.6-0.7")
        .when(col("DefaultProbability") < 0.8, "0.7-0.8")
        .when(col("DefaultProbability") < 0.9, "0.8-0.9")
        .otherwise("0.9-1.0")
    )
)

predictions_to_save.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.fact_loan_predictions")

print("Predictions saved to gold.fact_loan_predictions")

