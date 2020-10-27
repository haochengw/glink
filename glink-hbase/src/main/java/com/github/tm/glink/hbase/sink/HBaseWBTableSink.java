package com.github.tm.glink.hbase.sink;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.avro.AvroPoint;
import com.github.tm.stindex.ByteArray;
import com.github.tm.stindex.temporal.ConcatenationTimeEncoding;
import com.github.tm.stindex.temporal.TimeEncoding;
import com.github.tm.stindex.temporal.TimePointDimensionDefinition;
import com.github.tm.stindex.temporal.data.TimeValue;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Properties;

/**
 * @author Wang Yue
 * */
public class HBaseWBTableSink<T extends Point> extends RichSinkFunction<T> {

  private transient Configuration configuration;
  private transient Connection connection;
  private transient Admin admin;

  private transient Table table;

  private transient TimeEncoding timeEncoding;

  private String[] attrName = {"PRS", "PRS_Sea", "WIN_S_Max", "WIN_S_Avg_2mi", "TEM", "RHU", "PRE_1h", "VIS", "tigan"};

  public String tableName;

  public HBaseWBTableSink(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
    configuration = HBaseConfiguration.create();
    connection = ConnectionFactory.createConnection(configuration);
    admin = connection.getAdmin();

    table = connection.getTable(TableName.valueOf(tableName));

    timeEncoding = new ConcatenationTimeEncoding(new TimePointDimensionDefinition(Calendar.HOUR));
  }

  @Override
  public void invoke(T value, SinkFunction.Context context) throws Exception {
    System.out.println(value);

    LocalDateTime localDateTime = Instant.ofEpochMilli(value.getTimestamp())
            .atZone(ZoneOffset.ofHours(8)).toLocalDateTime();
    TimeValue timeValue = new TimeValue(
            localDateTime.getYear(),
            localDateTime.getMonthValue(),
            localDateTime.getDayOfMonth(),
            localDateTime.getHour(),
            localDateTime.getMinute(),
            localDateTime.getSecond());
    ByteArray time = timeEncoding.getIndex(timeValue).getStart();
    ByteArray rowKey = (new ByteArray(value.getId())).combine(time);
    Put put = new Put(rowKey.getBytes());
    Properties attributes = value.getAttributes();
    for (String name : attrName) {
      // attributes.get(name)得到org.apache.avro.util.Utf8对象
      // 如果使用attributes.getProperty(name),因为对象类型不是String,会返回null
      put.addColumn("f".getBytes(), name.getBytes(), attributes.get(name).toString().getBytes());
    }
    table.put(put);
  }

  @Override
  public void close() throws Exception {
    table.close();
    admin.close();
    connection.close();
  }
}
