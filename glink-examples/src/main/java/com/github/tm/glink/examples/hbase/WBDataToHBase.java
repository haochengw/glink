package com.github.tm.glink.examples.hbase;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.serialization.FlinkPointDeSerialize;
import com.github.tm.glink.hbase.sink.HBaseWBTableSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * This class import weather base data from kafka broker to hbase table
 * Store to the corresponding table according to different row key encodings
 * The correspondence is as follows:
 * 1. rowkey: IDT(Station_ID yyyyMMddHH) -> tableName: Hubei_WeatherBase_IDT
 * @author Wang Yue
 * */
public class WBDataToHBase {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final String schema = "PRS:string;PRS_Sea:string;WIN_S_Max:string;WIN_S_Avg_2mi:string;TEM:string;RHU:string;PRE_1h:string;VIS:string;tigan:string";

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    FlinkKafkaConsumer<Point> consumer = new FlinkKafkaConsumer<>(
            "WeatherBaseData",
            new FlinkPointDeSerialize(schema),
            props);
    DataStream<Point> dataStream = env.addSource(consumer);
    dataStream.addSink(new HBaseWBTableSink<>("Hubei_WeatherBase_IDT"));

    dataStream.print();

    env.execute();
  }
}
