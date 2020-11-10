package com.github.tm.glink.hbase.query;

import com.github.tm.glink.features.NullPoint;
import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.hbase.geodatabase.DataTypeEnum;
import com.github.tm.glink.hbase.geodatabase.IndexTable;
import com.github.tm.glink.hbase.query.concurrency.TrajectorySTRangeQueryJob;
import com.github.tm.glink.hbase.query.queryCondition.QueryCondition;
import com.github.tm.glink.hbase.query.queryCondition.STRangeCondition;
import com.github.tm.stindex.ByteArray;
import com.github.tm.stindex.ByteArrayRange;
import com.github.tm.stindex.api.BoundingBox;
import com.github.tm.stindex.dimension.BasicDimensionDefinition;
import com.github.tm.stindex.spatial.GridIndex;
import com.github.tm.stindex.spatial.sfc.SFCDimensionDefinition;
import com.github.tm.stindex.spatial.sfc.SFCFactory;
import com.github.tm.stindex.spatial.sfc.data.BasicNumericDataset;
import com.github.tm.stindex.spatial.sfc.data.NumericData;
import com.github.tm.stindex.spatial.sfc.data.NumericRange;
import com.github.tm.stindex.st.ConcatenationEncoding;
import com.github.tm.stindex.st.STEncoding;
import com.github.tm.stindex.temporal.ConcatenationTimeEncoding;
import com.github.tm.stindex.temporal.TimeEncoding;
import com.github.tm.stindex.temporal.TimePointDimensionDefinition;
import com.github.tm.stindex.temporal.data.TimeData;
import com.github.tm.stindex.temporal.data.TimeRange;
import com.github.tm.stindex.temporal.data.TimeValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.LinkedTransferQueue;

public class STRangeQuery extends QueryOptions {
  private static final Logger LOGGER = LoggerFactory.getLogger(STRangeQuery.class);
  private transient Table table;
  private transient STEncoding timePointEncoding;
  private static TimeValue startTime;
  private static TimeValue endTime;

  public STRangeQuery() {
  }

  public STRangeQuery(final DataTypeEnum dataTypeEnum, final IndexTable indexTable) {
    super(dataTypeEnum, indexTable);
  }

  @Override
  public List executeQuery(QueryCondition queryCondition) throws Throwable {
    if (!(queryCondition instanceof STRangeCondition)) {
      LOGGER.error("Spatial temporal query need STRangeCondition");
      return null;
    }
    STRangeCondition condition = (STRangeCondition) queryCondition;

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


  private List<TrajectoryPoint> trajectoryClientQuery(STRangeCondition condition) throws Exception {
    LinkedTransferQueue<TrajectoryPoint> resultQueue = new LinkedTransferQueue<>();
    List<String> idList = condition.getIdList();
    String schema = condition.getAttributeList()[0];
    BoundingBox boundingBox = condition.getBoundingBox();
    LocalDateTime startDate = condition.getStartDate();
    LocalDateTime endDate = condition.getEndDate();
    if (condition.isCurrent()) {
      endDate = LocalDateTime.now(ZoneOffset.ofHours(8));
    }
    startTime = new TimeValue(
            startDate.getYear(),
            startDate.getMonthValue(),
            startDate.getDayOfMonth(),
            startDate.getHour(),
            startDate.getMinute(),
            startDate.getSecond());
    endTime = new TimeValue(
            endDate.getYear(),
            endDate.getMonthValue(),
            endDate.getDayOfMonth(),
            endDate.getHour(),
            endDate.getMinute(),
            endDate.getSecond());
    TimeData timePointRange = new TimeRange(startTime, endTime);
    NumericRange lonRange = new NumericRange(boundingBox.getMinLon(), boundingBox.getMaxLon());
    NumericRange latRange = new NumericRange(boundingBox.getMinLat(), boundingBox.getMaxLat());
    BasicNumericDataset spatialRange = new BasicNumericDataset(new NumericData[] {lonRange, latRange});

    TimePointDimensionDefinition timePointDimDef = new TimePointDimensionDefinition(Calendar.DAY_OF_MONTH);
    TimeEncoding timePointTimeEncoding = new ConcatenationTimeEncoding(timePointDimDef);
    SFCDimensionDefinition[] dimensions = {
            new SFCDimensionDefinition(new BasicDimensionDefinition(-180.0, 180.0), 20),
            new SFCDimensionDefinition(new BasicDimensionDefinition(-90.0, 90.0), 20)};
    GridIndex zorder = SFCFactory.createSpaceFillingCurve(dimensions, SFCFactory.SFCType.ZORDER);
    timePointEncoding = new ConcatenationEncoding(timePointTimeEncoding, zorder);
    // If there are too many ranges, the bitsOfPrecision can be reduced
    List<ByteArrayRange> ranges = Arrays.asList(timePointEncoding.decomposeRanges(timePointRange, spatialRange));

    int perSplitNum = ranges.size() / threadNum;
    for (int i = 0; i < threadNum; i++) {
      int start = i * perSplitNum;
      int end = (i + 1) * perSplitNum;
      queryWorkersPool.execute(new TrajectorySTRangeQueryJob(
              indexTable.getTable(),
              ranges,
              start,
              end,
              boundingBox,
              resultQueue,
              startTime,
              endTime,
              schema,
              idList));
    }
    queryWorkersPool.execute(new TrajectorySTRangeQueryJob(
            indexTable.getTable(),
            ranges,
            perSplitNum * threadNum,
            ranges.size(),
            boundingBox,
            resultQueue,
            startTime,
            endTime,
            schema,
            idList));

    List<TrajectoryPoint> results = new ArrayList<>();
    int count = 0;
    while (true) {
      TrajectoryPoint point = resultQueue.take();
      if (point instanceof NullPoint) {
        if (++count == threadNum + 1) break;
      } else {
        results.add(point);
      }
    }
    return results;
  }

  private List<String> weatherClientQuery(STRangeCondition condition) throws IOException {
    table = indexTable.getTable();
    List<String> idList = new LinkedList<>();
    BoundingBox boundingBox = condition.getBoundingBox();
    double[] minLonLat = new double[]{boundingBox.getMinLon(), boundingBox.getMinLat()};
    double[] maxLonLat = new double[]{boundingBox.getMaxLon(), boundingBox.getMaxLat()};
    SFCDimensionDefinition[] dimensions = {
            new SFCDimensionDefinition(new BasicDimensionDefinition(-180.0, 180.0), 20),
            new SFCDimensionDefinition(new BasicDimensionDefinition(-90.0, 90.0), 20)};
    GridIndex zorder = SFCFactory.createSpaceFillingCurve(dimensions, SFCFactory.SFCType.ZORDER);
    ByteArray startKey = zorder.getIndex(minLonLat);
    ByteArray endKey = zorder.getIndex(maxLonLat);

    Scan scan = new Scan();
    scan.withStartRow(startKey.getBytes(), true);
    scan.withStopRow(endKey.getBytes(), true);
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("id"));
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("lat"));
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("lng"));
    ResultScanner resultScanner = table.getScanner(scan);
    for (Result result : resultScanner) {
      Cell[] cells = result.rawCells();
      double lat = Bytes.toDouble(CellUtil.cloneValue(cells[1]));
      double lng = Bytes.toDouble(CellUtil.cloneValue(cells[2]));
      if (lat <= boundingBox.getMaxLat() && lat >= boundingBox.getMinLat()
              && lng <= boundingBox.getMaxLon() && lng >= boundingBox.getMinLon()) {
        idList.add(new String(CellUtil.cloneValue(cells[0])));
      }
    }
    return idList;
  }
}
