#!/bin/bash

cd /mnt/c/hadoop_batch_processing
source venv/bin/activate
echo "[INFO] Hadoop Batch Processing is Running ..."
python3 app.py
filetime=$(date +"%Y%m%d")
echo "[INFO] MapReduce is Running on Local ..."
mkdir -p output
python3 map_reduce.py /mnt/c/hadoop_batch_processing/local/dim_orders_$filetime.csv > /mnt/c/hadoop_batch_processing/output/order_sum_output_local_map.txt
echo "[INFO] MapReduce is Running on Hadoop ..."
python3 map_reduce.py -r hadoop hdfs:///dim_orders_$filetime.csv > /mnt/c/hadoop_batch_processing/output/order_sum_output_hadoop_map.txt
echo "[INFO] Hadoop Batch Processing is Done ..."