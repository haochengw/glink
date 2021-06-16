package com.github.tm.glink.examples.demo.xiamen;

import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.examples.utils.HBaseCatalogCleaner;
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
    public static final String FILEPATH = "/mnt/hgfs/disk/dcic/data/origin/XiaMen2019DuanWu.csv";
    public static final String TOPICID = "XiamenData";
    public static final int SPEED_UP = 20;
    public static final int TIMEFIELDINDEX = 3;
    public static final TextFileSplitter SPLITTER = TextFileSplitter.CSV;
    public static final String CATALOG_NAME = "Xiamen";
    public static final String TILE_SCHEMA_NAME = "Heatmap";
    public static final String POINTS_SCHEMA_NAME = "JoinedPoints";

    public static void main(String[] args) throws Exception {
        // Drop old tables in HBase
        new HBaseCatalogCleaner(XiamenTwoJobs.ZOOKEEPERS).deleteTable(CATALOG_NAME, TILE_SCHEMA_NAME);
        new HBaseCatalogCleaner(XiamenTwoJobs.ZOOKEEPERS).deleteTable(CATALOG_NAME, POINTS_SCHEMA_NAME);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", XiamenTwoJobs.KAFKA_BOOSTRAP_SERVERS);
        props.put("zookeeper.connect", XiamenTwoJobs.ZOOKEEPERS);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        env.addSource(new CSVStringSourceSimulation(FILEPATH, SPEED_UP, TIMEFIELDINDEX, SPLITTER, false))
                .addSink(new FlinkKafkaProducer<String>(
                        TOPICID,
                        new SimpleStringSchema(),
                        props)).disableChaining();
        env.execute();
    }
}
