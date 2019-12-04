# copyright 2019 Martin Lurie
# sample code not supported
# use pairplot and logistic regression to 
# predict if a claim will be disputed



from __future__ import print_function
!echo $PYTHON_PATH
import os, sys
#import path
from pyspark.sql import *
from pyspark.sql.types import *

# # create spark sql session
myspark = SparkSession\
    .builder\
    .appName("Abrechnungen_Analyze") \
    .getOrCreate()

sc = myspark.sparkContext

import datetime
import time
start_time = datetime.datetime.now().time().strftime('%H:%M:%S')

sc.setLogLevel("ERROR")
print ( myspark )
# make spark print text instead of octal
myspark.sql("SET spark.sql.parquet.binaryAsString=true")

# read in the data file from HDFS
#cmsdf = myspark.read.parquet ( "/user/hive/warehouse/cms.db/cmsml")

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

sample_data = cmsdf.sample(False, 0.01, 83).toPandas()
sample_data.transpose().head(23)

# # Feature Visualization
# 

numeric_cols = ["auszahlung_euro"]
categorical_cols = ["arzt", "fachgebiet", "krankenhaus_oder_arzt", "medikament", 
                    "bundesland", "auszahlung_monat"]


get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt
import seaborn as sns
import pandas


# # Verteilung Auszahlungen < 50€ 
#

payments_less_50 = sample_data[sample_data["auszahlung_euro"] < 50 ]

ax = sns.distplot(payments_less_50["auszahlung_euro"].dropna(), kde=False, hist=True,)


sns.boxplot(x="strittig", y="auszahlung_euro", data=sample_data)


cms1000=myspark.sql('select fachgebiet, krankenhaus_oder_arzt, medikament, strittig, bundesland, auszahlung_monat, auszahlung_euro from cmsdata where auszahlung_euro <  10000 limit 10000')
cms1000.show(3)
# seaborn wants a pandas dataframe, not a spark dataframe
# so convert
pdsdf = sample_data.toPandas()


sns.set(style="ticks" , color_codes=True)
# this takes a long time to run:  
# you can see it if you uncomment it
g = sns.pairplot(pdsdf,  hue="strittig" )

# predict if a payment will be disputed

# we can skip this step since we used Impala to make the 
# data numeric and normalize
# need to convert from text field to numeric
# this is a common requirement when using sparkML
#from pyspark.ml.feature import StringIndexer
# this will convert each unique string into a numeric
#indexer = StringIndexer(inputCol="txtlabel", outputCol="label")
#indexed = indexer.fit(mydf).transform(mydf)
#indexed.show(5)
# now we need to create  a  "label" and "features"
# input for using the sparkML library

cmsdispute=myspark.sql('select strittig label, fachgebiet, krankenhaus_oder_arzt, medikament, bundesland, auszahlung_monat, auszahlung_euro from cmsdata')

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors

assembler = VectorAssembler(
    inputCols=[ "fachgebiet", "krankenhaus_oder_arzt", "medikament", "bundesland", "auszahlung_monat", "auszahlung_euro"],
    outputCol="features")
output = assembler.transform(cmsdispute)
# note the column headers - label and features are keywords
print ( output.show(3) )
from pyspark.ml.classification import LogisticRegression

# Create a LogisticRegression instance. This instance is an Estimator.
lr = LogisticRegression(maxIter=10, regParam=0.01)
# Print out the parameters, documentation, and any default values.
print("LogisticRegression Parameter:\n" + lr.explainParams() + "\n")

# Learn a LogisticRegression model. This uses the parameters stored in lr.
model1 = lr.fit(output)

#### Major shortcut - no train and test data!!!
# Since model1 is a Model (i.e., a transformer produced by an Estimator),
# we can view the parameters it used during fit().
# This prints the parameter (name: value) pairs, where names are unique IDs for this
# LogisticRegression instance.
print("Model 1 fitting Phase beendet")
#print(model1.extractParamMap())

trainingSummary = model1.summary

# Obtain the objective per iteration
objectiveHistory = trainingSummary.objectiveHistory
print("Training Objective History:")
for objective in objectiveHistory:
    print(objective)

# Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
print("Training Summary (FPR, TPR) :")
trainingSummary.roc.show()

print("areaUnderROC: " + str(trainingSummary.areaUnderROC))

prediction = model1.transform(output)
prediction.show(3)
result = prediction.select("label", "probability", "prediction") \
    .collect()

#print(result)
i=0
for row in result:
   if ( row.label != row.prediction ):
    print("label=%s, prob=%s, prediction=%s" \
          % (row.label, row.probability, row.prediction))
    i=i+1
    if ( i > 10):
      break

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

trainingSummary.roc.show()
print("areaUnderROC: " + str(trainingSummary.areaUnderROC))    


# Now try with only 2 predictors
assembler = VectorAssembler(
    inputCols=[ "fachgebiet", "medikament"],
    outputCol="features")
output = assembler.transform(cmsdispute)
model2 = lr.fit(output)
trainingSummary = model2.summary

# Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
trainingSummary.roc.show()
print("areaUnderROC: " + str(trainingSummary.areaUnderROC))
    
import pickle
import cdsw

# Output
filename = 'model.pkl'
pickle.dump(model1, open(filename, 'wb'))
cdsw.track_file(filename)


end_time = datetime.datetime.now().time().strftime('%H:%M:%S')
total_time=(datetime.datetime.strptime(end_time,'%H:%M:%S') - datetime.datetime.strptime(start_time,'%H:%M:%S'))
print ( "Gesamt Laufzeit: " + str(total_time) )
