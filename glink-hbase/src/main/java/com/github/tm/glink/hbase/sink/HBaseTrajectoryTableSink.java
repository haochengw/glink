package com.github.tm.glink.hbase.sink;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.avro.AvroPoint;
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
import com.github.tm.stindex.temporal.TimePointDimensionDefinition;
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

  private transient STEncoding tstEncoding;

  private transient AvroPoint avroPoint;

  public String trajectoryPointTST;
  public String trajectoryPointIDTST;

  public HBaseTrajectoryTableSink(String trajectoryPointTST, String trajectoryPointIDTST) {
    this.trajectoryPointTST = trajectoryPointTST;
    this.trajectoryPointIDTST = trajectoryPointIDTST;
  }

  @Override
  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
    configuration = HBaseConfiguration.create();
    connection = ConnectionFactory.createConnection(configuration);
    admin = connection.getAdmin();

    table1 = connection.getTable(TableName.valueOf(trajectoryPointTST));
    table2 = connection.getTable(TableName.valueOf(trajectoryPointIDTST));

    TimeDimensionDefinition timePeriodDimDef = new TimePeriodDimensionDefinition(Calendar.SECOND);
    TimeEncoding timePeriodTimeEncoding = new ConcatenationTimeEncoding(timePeriodDimDef);
    SFCDimensionDefinition[] dimensions = {
            new SFCDimensionDefinition(new BasicDimensionDefinition(-180.0, 180.0), 2),
            new SFCDimensionDefinition(new BasicDimensionDefinition(-90.0, 90.0), 2)};
    GridIndex hilbert = SFCFactory.createSpaceFillingCurve(dimensions, SFCFactory.SFCType.HILBERT);

    tstEncoding = new ConcatenationEncoding(timePeriodTimeEncoding, hilbert);

    avroPoint = new AvroPoint();
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
    ByteArray rowKey1 = tstEncoding.getIndex(timeValue, new double[] {value.getLng(), value.getLat()});
    ByteArray rowKey2 = (new ByteArray(value.getId())).combine(rowKey1);

    Put put1 = new Put(rowKey1.getBytes());
    put1.addColumn("f".getBytes(), "v".getBytes(), avroPoint.serialize(value));
    table1.put(put1);

    Put put2 = new Put(rowKey2.getBytes());
    put2.addColumn("f".getBytes(), "v".getBytes(), avroPoint.serialize(value));
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