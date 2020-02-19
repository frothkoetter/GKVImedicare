# # Visualisation in CDSW
# Workbench is based on iPython and as such supports the most popular visualisation
# frameworks available for Python or R. 
# ### Known Limitation : 
#  - no support for ipywidgets
#  - some visualisation must be imported as IFrames ( ex some plotly graphs ) 
#  - single line evaluation
 

# ## **Load data**
# We'll be using Spark to access data for 2 reasons : 
# - Integration with the CDH and HDP platforms
# - Distributed computing : 
#   When working with large dataset, it is often impossible to visualise the entire
#   dataset directly. 
#   Pre-processing must be done to reduce dimensionality to a size "acceptable" for
#   most visualisation libraries 
#   -  Agregation 
#   -  Sampling



# ### Start Spark session
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
  .master('yarn') \
  .config("spark.executor.instances","3")\
  .config("spark.executor.memory","2g")\
  .appName('visualisation_workbench') \
  .getOrCreate()


# ### Acess data ( prepared by the setup.sh script )
# The table contains a fairly "large" dataset ( 5.2 M lines ).
# Based on ASA airline on-time dataset [http://stat-computing.org/dataexpo/2009/]
# - using Year 1988

# ### From Hive 
spark.sql('''describe table cms.generalpayments''').show(50)
cms_abrechnungen_df = spark.sql('select * from cms.abrechnungen')


cms_abrechnungen_df.cache()
cms_abrechnungen_df.createOrReplaceTempView('abrechnungen')
cms_abrechnungen_df.printSchema()


# ### Simple data quality analysis
# #### Number of rows 
print("\nDataset has : {} rows".format(cms_abrechnungen_df.count()))

# #### Number of null values for each columns 
for col in cms_abrechnungen_df.columns: 
  count = cms_abrechnungen_df.filter(cms_abrechnungen_df[col].isNull()).count()
  print('{} has {} nulls'.format(col,count))


# ## **Visual analysis**
# Most visualisation will fail for large volumes > ~500k/1M
# Ex : Trying to bring the data back as a Pandas Dataframe will crash the driver

# ### Approach 1 - Sampling 
# #### Question 1 : Departure delay distribution
# using seaborn

pandas_df_payout = cms_abrechnungen_df\
  .select(['auszahlung_euro'])\
  .filter(cms_abrechnungen_df['auszahlung_euro'] < 100 )\
  .sample(False, 0.1 , seed=30)\
  .toPandas()
pandas_df_payout.info()


%matplotlib inline
import matplotlib.pyplot as plt
import seaborn as sns

# ##### Limitation : single line for plots 
sns.distplot(pandas_df_payout['auszahlung_euro'],kde=False, color='red', bins=100 )\
  .set( title="Basic DistPlot")
  
  
# spark.stop()
