from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder \
      .appName("Telco Customer Churn") \
      .master("local[*]") \
      .getOrCreate()
  
model = PipelineModel.load("file:///home/cdsw/models/cms_model_spark") 


features = ["fachgebiet", "krankenhaus_oder_arzt", "medikament", 
                       "bundesland", "auszahlung_monat", "auszahlung_euro" ]

def predict(args):
    account=args["feature"].split(",")
    feature = spark.createDataFrame([account[:1] + list(map(float,account[1:6]))], features)
    
    result=model.transform(feature).collect()[0].prediction
    return {"result" : result}
