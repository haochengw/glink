package com.github.tm.glink.examples.hbase;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.features.avro.AvroPoint;
import com.github.tm.glink.features.serialization.FlinkPointDeSerialize;
import com.github.tm.glink.features.serialization.FlinkTrajectoryDeSerialize;
import com.github.tm.glink.hbase.sink.HBaseTrajectoryTableSink;
import com.github.tm.glink.hbase.sink.HBaseWBTableSink;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * This class import Xiamen taxi trajectory point data from kafka broker to hbase table
 * Store to the corresponding table according to different row key encodings
 * The correspondence is as follows:
 * 1. rowkey: TST(yyyyMMdd Z2 HHmmss) -> tableName: Xiamen_TrajectoryPoint_TST
 * 2. rowkey: ID + TST(yyyyMMdd Z2 HHmmss) -> tableName: Xiamen_TrajectoryPoint_IDTST
 *
 * @author Wang Yue
 * */
public class XiamenTrajectoryToHBase {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final String schema = "speed:double;azimuth:int;status:int";

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    FlinkKafkaConsumer<TrajectoryPoint> consumer = new FlinkKafkaConsumer<>(
            "XiamenTrajectory",
            new FlinkTrajectoryDeSerialize(schema),
            props);
    DataStream<TrajectoryPoint> dataStream = env.addSource(consumer);
    dataStream.addSink(new HBaseTrajectoryTableSink<>("Xiamen_TrajectoryPoint_TST", "Xiamen_TrajectoryPoint_IDTST"));

    dataStream.print();

    env.execute();
  }
}