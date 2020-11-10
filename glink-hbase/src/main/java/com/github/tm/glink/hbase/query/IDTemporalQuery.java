package com.github.tm.glink.hbase.query;

import com.alibaba.fastjson.JSONObject;
import com.github.tm.glink.features.NullPoint;
import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.hbase.geodatabase.DataTypeEnum;
import com.github.tm.glink.hbase.geodatabase.IndexTable;
import com.github.tm.glink.hbase.query.concurrency.TrajectoryIDTemporalQueryJob;
import com.github.tm.glink.hbase.query.concurrency.WeatherIDTemporalQueryJob;
import com.github.tm.glink.hbase.query.queryCondition.IDTemporalCondition;
import com.github.tm.glink.hbase.query.queryCondition.QueryCondition;
import com.github.tm.stindex.ByteArray;
import com.github.tm.stindex.ByteArrayRange;
import com.github.tm.stindex.temporal.ConcatenationTimeEncoding;
import com.github.tm.stindex.temporal.TimeEncoding;
import com.github.tm.stindex.temporal.TimePointDimensionDefinition;
import com.github.tm.stindex.temporal.data.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedTransferQueue;

public class IDTemporalQuery extends QueryOptions {
  private static final Logger LOGGER = LoggerFactory.getLogger(STRangeQuery.class);
  private transient TimeEncoding timeEncoding;
  private static TimeValue startTime;
  private static TimeValue endTime;

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

  public List<TrajectoryPoint> trajectoryClientQuery(IDTemporalCondition condition) throws Exception {
    timeEncoding = new ConcatenationTimeEncoding(new TimePointDimensionDefinition(Calendar.DATE));
    LinkedTransferQueue<TrajectoryPoint> resultQueue = new LinkedTransferQueue<>();
    List<ByteArrayRange> ranges = new LinkedList<>();
    List<String> idList = condition.getIdList();
    String schema = condition.getAttributeList()[0];
    // Type1: ID Query.
    if (condition.getStartDate() == null && condition.getEndDate() == null) {
      for (String id : idList) {
        ByteArrayRange range = new ByteArrayRange(id.getBytes(), id.getBytes(), true);
        ranges.add(range);
      }
    } else {
      // Type2: ID Temporal Query.
      LocalDateTime startDate = condition.getStartDate();
      LocalDateTime endDate = condition.getEndDate();
      // Type2.1: ID Temporal Query.(to current)
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
      // Type2.2: ID Temporal Query.(Minimum time bin: year/month/day)
      // Type2.3: ID Temporal Query.(Minimum time bin: hour/minute/second)
      // @see: TrajectoryIDTemporalQueryJob
      if (startDate.getYear() == startDate.getYear()
              && startDate.getMonthValue() == startDate.getMonthValue()
              && startDate.getDayOfMonth() == startDate.getDayOfMonth()) {
        for (String id : idList) {
          ByteArrayRange range = new ByteArrayRange(
                  (new ByteArray(id).combine(timeEncoding.getIndex(startTime).getStart())).getBytes(),
                  (new ByteArray(id).combine(timeEncoding.getIndex(startTime).getStart())).getBytes(),
                  true);
          ranges.add(range);
        }
      } else {
        for (String id : idList) {
          ByteArrayRange range = new ByteArrayRange(
                  (new ByteArray(id).combine(timeEncoding.getIndex(startTime).getStart())).getBytes(),
                  (new ByteArray(id).combine(timeEncoding.getIndex(endTime).getStart())).getBytes(),
                  false);
          ranges.add(range);
        }
      }
    }

    int perSplitNum = ranges.size() / threadNum;
    for (int i = 0; i < threadNum; i++) {
      int start = i * perSplitNum;
      int end = (i + 1) * perSplitNum;
      queryWorkersPool.execute(new TrajectoryIDTemporalQueryJob(
              indexTable.getTable(),
              ranges,
              start,
              end,
              resultQueue,
              startTime,
              endTime,
              schema));
    }
    queryWorkersPool.execute(new TrajectoryIDTemporalQueryJob(
            indexTable.getTable(),
            ranges,
            perSplitNum * threadNum,
            ranges.size(),
            resultQueue,
            startTime,
            endTime,
            schema));

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

  private List<JSONObject> weatherClientQuery(IDTemporalCondition condition) throws Exception {
    timeEncoding = new ConcatenationTimeEncoding(new TimePointDimensionDefinition(Calendar.HOUR));
    LinkedTransferQueue<JSONObject> resultQueue = new LinkedTransferQueue<>();
    List<ByteArrayRange> ranges = new LinkedList<>();
    List<String> idList = condition.getIdList();
    String[] attributeList = condition.getAttributeList();
    // Type1: ID Query.
    if (condition.getStartDate() == null && condition.getEndDate() == null) {
      for (String id : idList) {
        ByteArrayRange range = new ByteArrayRange(id.getBytes(), id.getBytes(), true);
        ranges.add(range);
      }
    } else {
      // Type2: ID Temporal Query.
      LocalDateTime startDate = condition.getStartDate();
      LocalDateTime endDate = condition.getEndDate();
      // Type2.1: ID Temporal Query.(to current)
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
      // Type2.2: ID Temporal Query.(Minimum time bin: year/month/day)
      for (String id : idList) {
        ByteArrayRange range = new ByteArrayRange(
                (new ByteArray(id).combine(timeEncoding.getIndex(startTime).getStart())).getBytes(),
                (new ByteArray(id).combine(timeEncoding.getIndex(endTime).getStart())).getBytes(),
                false);
        ranges.add(range);
      }
    }

    int threadCount = 0;
    int perSplitNum = ranges.size() / threadNum;
    for (int i = 0; i < threadNum; i++) {
      int start = i * perSplitNum;
      int end = (i + 1) * perSplitNum;
      if (start != end) {
        queryWorkersPool.execute(new WeatherIDTemporalQueryJob(
                indexTable.getTable(),
                ranges,
                start,
                end,
                resultQueue,
                attributeList));
      } else {
        threadCount++;
      }
    }
    queryWorkersPool.execute(new WeatherIDTemporalQueryJob(
            indexTable.getTable(),
            ranges,
            perSplitNum * threadNum,
            ranges.size(),
            resultQueue,
            attributeList));

    List<JSONObject> json = new ArrayList<>();
    while (true) {
      JSONObject re = resultQueue.take();
      if (re.isEmpty()) {
        if (++threadCount == threadNum + 1) break;
      } else {
        json.add(re);
      }
    }
    return json;
  }
}
