package com.github.tm.glink.hbase.sink;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.features.avro.AvroTrajectoryPoint;
import com.github.tm.stindex.ByteArray;
import com.github.tm.stindex.dimension.BasicDimensionDefinition;
import com.github.tm.stindex.dimension.TimeDimensionDefinition;
import com.github.tm.stindex.spatial.GridIndex;
import com.github.tm.stindex.spatial.sfc.SFCDimensionDefinition;
import com.github.tm.stindex.spatial.sfc.SFCFactory;
import com.github.tm.stindex.st.ConcatenationEncoding;
import com.github.tm.stindex.st.STEncoding;
import com.github.tm.stindex.temporal.ConcatenationTimeEncoding;
import com.github.tm.stindex.temporal.TimeEncoding;
import com.github.tm.stindex.temporal.TimePeriodDimensionDefinition;
import com.github.tm.stindex.temporal.data.TimeValue;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Properties;

/**
 * This class is used to design the storage structure for the trajectory point data
 * Each table corresponds to a storage structure
 * @author Wang Yue
 * */
public class HBaseTrajectoryTableSink<T extends Point> extends RichSinkFunction<T> {
  private transient Configuration configuration;
  private transient Connection connection;
  private transient Admin admin;
  private transient Table table1;
  private transient Table table2;

  public String tableName1;
  public String tableName2;

  private transient STEncoding tstEncoding;

  private transient AvroTrajectoryPoint avroTrajectoryPoint;


  public HBaseTrajectoryTableSink(String tableName1, String tableName2) {
    this.tableName1 = tableName1;
    this.tableName2 = tableName2;
  }

  @Override
  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
    configuration = HBaseConfiguration.create();
    connection = ConnectionFactory.createConnection(configuration);
    admin = connection.getAdmin();

    table1 = connection.getTable(TableName.valueOf(tableName1));
    table2 = connection.getTable(TableName.valueOf(tableName2));

    TimeDimensionDefinition timePeriodDimDef = new TimePeriodDimensionDefinition(Calendar.SECOND);
    TimeEncoding timePeriodTimeEncoding = new ConcatenationTimeEncoding(timePeriodDimDef);
    SFCDimensionDefinition[] dimensions = {
            new SFCDimensionDefinition(new BasicDimensionDefinition(-180.0, 180.0), 20),
            new SFCDimensionDefinition(new BasicDimensionDefinition(-90.0, 90.0), 20)};
    GridIndex zorder = SFCFactory.createSpaceFillingCurve(dimensions, SFCFactory.SFCType.ZORDER);

    tstEncoding = new ConcatenationEncoding(timePeriodTimeEncoding, zorder);

    String schema = "speed:double;azimuth:int;status:int";
    avroTrajectoryPoint = new AvroTrajectoryPoint(schema);
  }

  @Override
  public void invoke(T value, Context context) throws Exception {
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
    ByteArray rowKey = tstEncoding.getIndex(timeValue, new double[] {value.getLng(), value.getLat()});
    // TST + ID
    ByteArray rowKey1 = rowKey.combine(new ByteArray(value.getId()));
    // ID + TST
    ByteArray rowKey2 = (new ByteArray(value.getId())).combine(rowKey);

    Put put1 = new Put(rowKey1.getBytes());
    put1.addColumn("f".getBytes(), "v".getBytes(), avroTrajectoryPoint.serialize((TrajectoryPoint) value));
    table1.put(put1);

    Put put2 = new Put(rowKey2.getBytes());
    put2.addColumn("f".getBytes(), "v".getBytes(), avroTrajectoryPoint.serialize((TrajectoryPoint) value));
    table2.put(put2);
  }

  @Override
  public void close() throws Exception {
    table1.close();
    table2.close();
    admin.close();
    connection.close();
  }
}