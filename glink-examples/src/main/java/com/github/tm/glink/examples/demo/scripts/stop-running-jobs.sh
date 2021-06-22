##!/bin/bash
response=`flink list`
source /etc/profile
echo $response
# 获取jobid
jobs=`echo $response | grep -E '[[:alnum:]]{32}' -o`
jobids=(${jobs// /})
for jobid in $jobids
do
    flink cancel $jobid
done