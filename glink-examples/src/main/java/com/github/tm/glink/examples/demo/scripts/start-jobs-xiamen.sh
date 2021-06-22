#!/bin/bash
## ----  Need edit ----
jar_path="/Users/haocheng/Code/glink/glink-examples/target/glink-examples-0.2-SNAPSHOT.jar"
main_class_path="com.github.tm.glink.examples.demo"
## ----  Need edit ----

source /etc/profile
jobs=("CleanUp" "Heatmap" "GeoFenceJoin" "KafkaDataProducer")
for job in ${jobs[*]}
do
    if [ $job == "CleanUp"  ]
    then
        java -cp $jar_path $main_class_path"."'xiamen'"."$job
    elif [ $job == "KafkaDataProducer" ]
    then
        java -cp $jar_path $main_class_path"."'xiamen'"."$job &
    else
        flink run -c $main_class_path"."'xiamen'"."$job -d $jar_path 
    fi
done

# Check count of jobs.
response=`flink list`
# 获取jobid
sleep 2s
jobs=`echo $response | grep -E '[[:alnum:]]{32}' -o`
jobids=(${jobs// /})
count=${#jobids[@]}
if [ $count -ne 2 ]
then
  echo "Not all jobs started, restarting flink cluster..."
  # 没有对应数量的job启动成功, 则重启flink cluster, 重新执行这个script.
  sh /opt/flink-restart.sh
  sh /opt/start-jobs-xiamen.sh
else
    echo "$count jobs are started."
fi