from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder \
      .appName("Telco Customer Churn") \
      .master("local[*]") \
      .getOrCreate()
  
model = PipelineModel.load("file:///home/cdsw/models/cms_model_spark") 


features = ["idx","fachgebiet", "krankenhaus_oder_arzt", "medikament", 
                       "bundesland", "auszahlung_monat", "auszahlung_euro" ]

def predict(args):
    parm=["feature",1.0, 2.0, 3.0, 4.0, 5.0, 6.0 ]
    feature = spark.createDataFrame( [parm ], features)
    feature.show()
    result=model.transform(feature).collect()[0].prediction
    return {"result" : result}