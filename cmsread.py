from __future__ import print_function
import sys, re, subprocess
from operator import add
from pyspark.sql import SparkSession
from __future__ import print_function



#Set the user name which will be used to login to the HDFS Master.
USER_NAME = subprocess.getoutput("klist | sed -n 's/.*principal://p' | cut -d@ -f1")
print (USER_NAME)
#FQDN of the HDFS Master
REMOTE_HDFS_MASTER = 'frothkoetter-data-engineering-cluster-master0.frothkoe.a465-9q4k.cloudera.site'

#Copy Hadoop config files into the CML session
!scp -o "StrictHostKeyChecking no" -T $USER_NAME@$REMOTE_HDFS_MASTER:"/etc/hadoop/conf/core-site.xml /etc/hadoop/conf/hdfs-site.xml" /etc/hadoop/conf

spark = SparkSession\
    .builder\
    .appName("RemoteHDFSAccess")\
    .config("spark.authenticate","true")\
    .getOrCreate()
  

  
#Create DataFrame
data = [('Anna', 1), ('John', 2), ('Martin', 3), ('Carol', 4), ('Hannah', 5)]
df_write = spark.createDataFrame(data)


#Read the data from remote HDFS
df_load = spark.read.parquet("hdfs://" + REMOTE_HDFS_MASTER + "/tmp/cmsml/*")

df_load.show()

spark.stop()


# Access the file  

#p_file = "/user/frothkoetter/cms/generalpayments/OP_DTL_GNRL_PGYR2017_P01172020.csv"
#df_load = spark.read.csv("hdfs://" + REMOTE_HDFS_MASTER + p_file )


# park.stop()