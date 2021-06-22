#!/bin/bash
## ----  Need edit ----
jar_path="/Users/haocheng/Code/glink/glink-examples/target/glink-examples-0.2-SNAPSHOT.jar"
main_class_path="com.github.tm.glink.examples.nyc"
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