package com.github.tm.glink.hbase.query;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.hbase.geodatabase.DataTypeEnum;
import com.github.tm.glink.hbase.geodatabase.GeoDatabase;
import com.github.tm.glink.hbase.geodatabase.IndexTable;
import com.github.tm.glink.hbase.query.queryCondition.IDTemporalCondition;
import com.github.tm.glink.hbase.query.queryCondition.QueryCondition;
import com.github.tm.stindex.ByteArray;
import com.github.tm.stindex.ByteArrayRange;
import com.github.tm.stindex.TemporalByteArray;
import com.github.tm.stindex.temporal.ConcatenationTimeEncoding;
import com.github.tm.stindex.temporal.TimeEncoding;
import com.github.tm.stindex.temporal.TimePointDimensionDefinition;
import com.github.tm.stindex.temporal.data.TimeValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.List;

public class IDTemporalQuery extends QueryOptions {
  private static final Logger LOGGER = LoggerFactory.getLogger(STRangeQuery.class);
  private transient Table table;
  private transient TimeEncoding timeEncoding;

  public IDTemporalQuery() {
  }

  public IDTemporalQuery(final DataTypeEnum dataTypeEnum, final IndexTable indexTable) {
    super(dataTypeEnum, indexTable);
  }

  @Override
  public List executeQuery(QueryCondition queryCondition) throws Throwable {
    if (!(queryCondition instanceof IDTemporalCondition)) {
      LOGGER.error("Spatial temporal query need STRangeCondition");
      return null;
    }
    IDTemporalCondition condition = (IDTemporalCondition) queryCondition;

    switch (this.dataTypeEnum) {
      case TRAJECTORY:
        return trajectoryClientQuery(condition);
      case WEATHER:
        return weatherClientQuery(condition);
      default:
        LOGGER.error("Query for invalidate feature type");
        return null;
    }
  }

  public List<Point> trajectoryClientQuery(IDTemporalCondition condition) throws Exception {
//    List<ByteArrayRange> ranges = coding.decomposeRange(condition.getSTBBox(), condition.getMaxRecursive());
//    int perSplitNum = ranges.size() / threadNum;
//    for (int i = 0; i < threadNum; i++) {
//      int start = i * perSplitNum;
//      int end = (i + 1) * perSplitNum;
//      queryWorkersPool.execute(new TrajectoryClientQueryJob(
//              indexTable.getTable(),
//              ranges,
//              start,
//              end,
//              condition.getSTBBox(),
//              CodingFactory.createCodingSchema(coding),
//              resultQueue));
//    }
//    queryWorkersPool.execute(new TrajectoryClientQueryJob(
//            indexTable.getTable(),
//            ranges,
//            perSplitNum * threadNum,
//            ranges.size(),
//            condition.getSTBBox(),
//            CodingFactory.createCodingSchema(coding),
//            resultQueue));
//
//    List<SerializeGeometry> results = new ArrayList<>();
//    int count = 0;
//    while (true) {
//      SerializeGeometry geometry = resultQueue.take();
//      if (geometry instanceof SerializeNullGeometry) {
//        if (++count == threadNum + 1) break;
//      } else {
//        results.add(geometry);
//      }
//    }
//    return results;
    return null;
  }

  private <T> List<T> weatherClientQuery(IDTemporalCondition condition) throws IOException {
    table = indexTable.getTable();
    timeEncoding = new ConcatenationTimeEncoding(new TimePointDimensionDefinition(Calendar.HOUR));
    Scan scan = new Scan();

    String id = condition.getId();
    String[] attributeList = condition.getAttributeList();
    for (String s : attributeList) {
      if (s == "allType") {
        break;
      }
      scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes(s));
    }

    if (condition.getStartDate() == null && condition.getEndDate() == null) {
      // To avoid scanning the entire table.
      scan.withStartRow(id.getBytes(), true);
      scan.setFilter(new PrefixFilter(id.getBytes()));
    } else {
      LocalDateTime startTime = condition.getStartDate();
      LocalDateTime endTime = condition.getEndDate();
      if (condition.isCurrent()) {
        endTime = LocalDateTime.now(ZoneOffset.ofHours(8));
      }
      TimeValue start = new TimeValue(
              startTime.getYear(),
              startTime.getMonthValue(),
              startTime.getDayOfMonth(),
              startTime.getHour(),
              startTime.getMinute(),
              startTime.getSecond());
      TimeValue end = new TimeValue(
              endTime.getYear(),
              endTime.getMonthValue(),
              endTime.getDayOfMonth(),
              endTime.getHour(),
              endTime.getMinute(),
              endTime.getSecond());
      ByteArray startKey = (new ByteArray(id).combine(timeEncoding.getIndex(start).getStart()));
      ByteArray endKey = (new ByteArray(id).combine(timeEncoding.getIndex(end).getStart()));
      scan.withStartRow(startKey.getBytes(), true);
      scan.withStopRow(endKey.getBytes(), true);
    }
    ResultScanner resultScanner = table.getScanner(scan);
    for (Result result : resultScanner) {
      Cell[] cells = result.rawCells();
      for (Cell cell : cells) {
        ByteArray rowkey = new ByteArray(CellUtil.cloneRow(cell));
        // 暂时直接打印在控制台，调试用
        System.out.println("ID:" + new String(rowkey.slice(0, 5).getBytes()) + " ");
        System.out.println("time:" + new TemporalByteArray(rowkey.slice(5, 10).getBytes()) + " ");
        System.out.println("attrName:" + new String(CellUtil.cloneQualifier(cell)) + " ");
        System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
      }
    }
    return null;
//    List<SerializeGeometry> results = new ArrayList<>();
//    int count = 0;
//    while (true) {
//      SerializeGeometry geometry = resultQueue.take();
//      if (geometry instanceof SerializeNullGeometry) {
//        if (++count == threadNum + 1) break;
//      } else {
//        results.add(geometry);
//      }
//    }
//    return results;
  }

  // test
  public static void main(String[] args) {
    GeoDatabase geoDatabase = GeoDatabase.getInstance();
    geoDatabase.openConnection();
    Table table = null;
    try {
      table = geoDatabase.getTable("Hubei_WeatherBase_IDT");
    } catch (IOException e) {
      e.printStackTrace();
    }
    IndexTable indexTable = new IndexTable(table);
    IDTemporalQuery query = new IDTemporalQuery(DataTypeEnum.WEATHER, indexTable);

    String id = "57249";
    String[] attr = {"PRS"};
    DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    LocalDateTime start = LocalDateTime.parse("2020-10-09 12:00:00", df);
    LocalDateTime end = LocalDateTime.parse("2020-10-10 08:00:00", df);
    IDTemporalCondition condition = IDTemporalCondition.idWithAttribute(id, attr);
    IDTemporalCondition condition1 = IDTemporalCondition.idWithAttributefromStartToNow(id, start, attr);
    IDTemporalCondition condition2 = IDTemporalCondition.idWithAttributefromContinuousTime(id, start, end, attr);
    try {
      List results = query.weatherClientQuery(condition2);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
