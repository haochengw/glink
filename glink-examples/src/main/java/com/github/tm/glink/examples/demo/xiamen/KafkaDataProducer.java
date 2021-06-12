package com.github.tm.glink.examples.demo.xiamen;

import com.github.tm.glink.core.enums.TextFileSplitter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import java.util.Properties;

/**
 * Producer，将txt文本发送至kafka topic，可以控制速度。
 * @author Wang Haocheng
 * @date 2021/6/12 - 2:00 下午
 */
public class KafkaDataProducer {
    public static final String FILEPATH = "/Users/haocheng/Code/glink/glink-examples/src/main/resources/XiamenTrajDataCleaned.csv";
    public static final String TOPICID = "XiamenData";
    public static final int SPEED_UP = 50;
    public static final int CARNO_FIELD_UDINDEX = 4;
    public static final int TIMEFIELDINDEX = 3;
    public static final TextFileSplitter SPLITTER = TextFileSplitter.CSV;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", XiamenHeatMap.KAFKA_BOOSTRAP_SERVERS);
        props.put("zookeeper.connect", XiamenHeatMap.ZOOKEEPERS);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        env.addSource(new CSVStringSourceSimulation(FILEPATH, SPEED_UP, TIMEFIELDINDEX, SPLITTER, false))
                .addSink(new FlinkKafkaProducer<String>(
                        TOPICID,
                        new SimpleStringSchema(),
                        props));
        env.execute();
    }
}
