#!/bin/sh

mkdir -p /home/cdsw/data/
cd /home/cdsw/data

wget http://download.cms.gov/openpayments/PGYR17_P011720.ZIP -O /home/cdsw/data/cms.zip
jar -xvf /home/cdsw/data/cms.zip  

hdfs dfs -mkdir -p cms/
hdfs dfs -put /home/cdsw/data/OP_DTL_GNRL_PGYR2017_P01172020.csv cms/generalpayments.csv

hdfs dfs -ls cms/


beeline -n hive -u 'jdbc:hive2://base-mn03.cdp.ocrc.io:10002/cms' -f cmsdb.ddl


pip3 install -r /home/cdsw/requirements.txt
