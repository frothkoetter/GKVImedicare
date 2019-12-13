

# # Demo GKVi Abrechnung ML 
# sample code not supported
# use pairplot and logistic regression to 
# predict if a claim will be disputed


import os, sys
import matplotlib.pyplot as plt
import seaborn as sns
import pandas
import datetime
import time
import numpy as np


from pyspark.sql import *
from pyspark.sql.types import *

start_time = datetime.datetime.now().time().strftime('%H:%M:%S')


# # create spark sql session
myspark = SparkSession\
    .builder\
    .appName("Demo GKVI Abrechnungen Analyze") \
    .getOrCreate()

sc = myspark.sparkContext


sc.setLogLevel("ERROR")
print ( myspark )
# make spark print text instead of octal
myspark.sql("SET spark.sql.parquet.binaryAsString=true")

# Read in the data 

cmsdf = myspark.read.parquet ( "/tmp/cmsml")

# # Basic DataFrame operations
# 
# Dataframes essentially allow you to express sql-like statements. We can filter, count, and so on. [DataFrame Operations documentation.](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframe-operations)

count = cmsdf.count()
disputed = cmsdf.filter(cmsdf.strittig == 1 ).count()
un_disputed = cmsdf.filter(cmsdf.strittig == 0 ).count()

print( "Gesamt Abgrechnungsfälle: %d, unstrittig: %d, strittig: %d " % (count, un_disputed, disputed))

# # Exploratory DS
# 
# The data vizualization workflow for large data sets is usually:
# 
# * Sample data so it fits in memory on a single machine.
# * Examine single variable distributions.
# * Examine joint distributions and correlations.
# * Look for other types of relationships.
# 
# [DataFrame#sample() documentation](http://people.apache.org/~pwendell/spark-releases/spark-1.5.0-rc1-docs/api/python/pyspark.sql.html#pyspark.sql.DataFrame.sample)

sample_data = cmsdf.sample(False, 0.01, 83).toPandas().dropna()
sample_data.transpose().head(23)

# # Feature Visualization
# 

numeric_cols = ["auszahlung_euro"]
categorical_cols = ["arzt", "fachgebiet", "krankenhaus_oder_arzt", "medikament", 
                    "bundesland", "auszahlung_monat"]


get_ipython().magic(u'matplotlib inline')

# # Verteilung Auszahlungen < 50€ 
#

payments_less_50 = sample_data[sample_data["auszahlung_euro"] < 50]

ax = sns.distplot(payments_less_50["auszahlung_euro"].dropna(), kde=False, hist=True,)

ax = sns.boxplot(x="strittig", y="auszahlung_euro", data=payments_less_50)


# draw by catagories

disputed = payments_less_50.query("strittig == '1'").dropna()
un_disputed = payments_less_50.query("strittig == '0'").dropna().sample(frac=0.1)


# Draw density plots 

sns.set(style="darkgrid")
ax = sns.kdeplot(disputed.bundesland, disputed.auszahlung_euro,
                 cmap="Reds", shade=True, shade_lowest=False)

ay = sns.kdeplot(un_disputed.bundesland, un_disputed.auszahlung_euro,
                 cmap="Blues", shade=True, shade_lowest=False)


# # Predict if a payment will be disputed

# we can skip this step since we used to make the 
# data numeric and normalize
# need to convert from text field to numeric
# this is a common requirement when using sparkML
#
#from pyspark.ml.feature import StringIndexer
# this will convert each unique string into a numeric
#indexer = StringIndexer(inputCol="txtlabel", outputCol="label")
#indexed = indexer.fit(mydf).transform(mydf)
#indexed.show(5)

# now we need to create  a  "label" and "features"
# input for using the sparkML library


from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors

assembler = VectorAssembler(
    inputCols=[ "fachgebiet", "krankenhaus_oder_arzt", "medikament", "bundesland", "auszahlung_monat", "auszahlung_euro"],
    outputCol="features")

# build training data 

train_data = cmsdf.withColumnRenamed("strittig", "label").sample(False, 0.01, 83).dropna()
output = assembler.transform(train_data)

from pyspark.ml.classification import LogisticRegression

# Create a LogisticRegression instance. This instance is an Estimator.
lr = LogisticRegression(maxIter=10, regParam=0.01)

# Print out the parameters, documentation, and any default values.
# print("LogisticRegression Parameter:\n" + lr.explainParams() + "\n")

# Learn a LogisticRegression model. This uses the parameters stored in lr.
model1 = lr.fit(output)

#### Major shortcut - no train and test data!!!
# Since model1 is a Model (i.e., a transformer produced by an Estimator),
# we can view the parameters it used during fit().
# This prints the parameter (name: value) pairs, where names are unique IDs for this
# LogisticRegression instance.
#print(model1.extractParamMap())

trainingSummary = model1.summary

# Obtain the objective per iteration
objectiveHistory = trainingSummary.objectiveHistory
# # Model Objection Performance
print("Training Objective History:")
# plt.plot(objectiveHistory)

# Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
# Plots True Positive Rate vs False Positive Rate for binary classification system
# 
# [More Info](https://en.wikipedia.org/wiki/Receiver_operating_characteristic)
# 
# TL;DR for AUROC:
#     * .90-1 = excellent (A)
#     * .80-.90 = good (B)
#     * .70-.80 = fair (C)
#     * .60-.70 = poor (D)
#     * .50-.60 = fail (F)
# 

print("areaUnderROC: " + str(trainingSummary.areaUnderROC))

# # ROC plot

roc = trainingSummary.roc.toPandas()
plt.plot(roc['FPR'],roc['TPR'])
plt.show()

# # RECALL plot

pr = trainingSummary.pr.toPandas()
plt.plot(pr['recall'],pr['precision'])
plt.show()

prediction = model1.transform(output)
result = prediction.select("label", "probability", "prediction") \
    .collect()

  
# Now try with only 2 predictors
assembler = VectorAssembler(
    inputCols=[ "fachgebiet", "medikament"],
    outputCol="features")
output = assembler.transform(train_data)
model2 = lr.fit(output)
trainingSummary = model2.summary

# Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.

print("areaUnderROC: " + str(trainingSummary.areaUnderROC))

# # ROC plot

roc = trainingSummary.roc.toPandas()
plt.plot(roc['FPR'],roc['TPR'])
plt.show()


end_time = datetime.datetime.now().time().strftime('%H:%M:%S')
total_time=(datetime.datetime.strptime(end_time,'%H:%M:%S') - datetime.datetime.strptime(start_time,'%H:%M:%S'))
print ( "Gesamt Laufzeit: " + str(total_time) )


myspark.stop()