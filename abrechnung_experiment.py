from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import trim
import pandas as pd
import cdsw
import os

param_numIter=int(sys.argv[1])    # 10
param_regParam=float(sys.argv[2]) # 0.01


# # create spark sql session
myspark = SparkSession\
    .builder\
    .appName("Abrechnungen_Analyze_exp") \
    .getOrCreate()

sc = myspark.sparkContext


sc.setLogLevel("ERROR")
myspark.sql("SET spark.sql.parquet.binaryAsString=true")

# Read in the data 

cmsdf = myspark.read.parquet ( "/tmp/cmsml")

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer


label_indexer = StringIndexer(inputCol = 'strittig', outputCol = 'label')
print (label_indexer)

#plan_indexer = StringIndexer(inputCol = 'intl_plan', outputCol = 'intl_plan_indexed')


assembler = VectorAssembler(
    inputCols=[ "fachgebiet", "krankenhaus_oder_arzt", "medikament", "bundesland", "auszahlung_monat", "auszahlung_euro"],
    outputCol="features")


from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(maxIter=param_numIter, regParam=param_regParam)

pipeline = Pipeline(stages=[label_indexer, assembler, lr])

(train, test) = cmsdf.randomSplit([0.7, 0.3])
model = pipeline.fit(train.dropna())


  
cdsw.track_metric("numIter",param_numIter)
cdsw.track_metric("regParam",param_regParam)


from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import udf

predictions = model.transform(test.dropna())
evaluator = RegressionEvaluator()

auroc = evaluator.evaluate(predictions)
aupr = evaluator.evaluate(predictions)
"The AUROC is %s and the AUPR is %s" % (auroc, aupr)

cdsw.track_metric("auroc", auroc)
cdsw.track_metric("aupr", aupr)

model.write().overwrite().save("/tmp/cms_model_spark")

!rm -r -f models/spark
!rm -r -f models/spark_rf.tar
!hdfs dfs -get /tmp/cms_model_spark models
!tar -cvf models/spark_rf.tar models/spark

cdsw.track_file("models/spark_rf.tar")

myspark.stop()