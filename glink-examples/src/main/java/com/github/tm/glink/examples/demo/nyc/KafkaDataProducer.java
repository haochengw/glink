package com.github.tm.glink.examples.demo.nyc;

import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.examples.demo.xiamen.CSVStringSourceSimulation;
import com.github.tm.glink.examples.demo.xiamen.XiamenTwoJobs;
import com.github.tm.glink.examples.utils.HBaseCatalogCleaner;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author Wang Haocheng
 * @date 2021/6/16 - 4:54 下午
 */
public class KafkaDataProducer {
  public static final String FILEPATH = "/Users/haocheng/Code/glink/glink-examples/src/main/resources/NYCExample.txt";
  public static final String TOPICID = "NYCdata";
  public static final int SPEED_UP = 1;
  public static final int TIMEFIELDINDEX = 0;
  public static final TextFileSplitter SPLITTER = TextFileSplitter.CSV;
  public static final String CATALOG_NAME = NYCHeatmap.CATALOG_NAME;
  public static final String TILE_SCHEMA_NAME = "Heatmap";
  public static final String POINTS_SCHEMA_NAME = "JoinedPoints";

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Properties props = new Properties();
    props.put("bootstrap.servers", NYCHeatmap.KAFKA_BOOSTRAP_SERVERS);
    props.put("zookeeper.connect", NYCHeatmap.ZOOKEEPERS);
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
