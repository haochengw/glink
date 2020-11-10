package com.github.tm.glink.hbase.query.concurrency;

import com.github.tm.glink.features.NullPoint;
import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.features.avro.AvroTrajectoryPoint;
import com.github.tm.stindex.ByteArray;
import com.github.tm.stindex.ByteArrayRange;
import com.github.tm.stindex.TemporalByteArray;
import com.github.tm.stindex.api.BoundingBox;
import com.github.tm.stindex.temporal.data.TimeValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.util.List;
import java.util.concurrent.LinkedTransferQueue;

/**
 * Trajectory Spatial Temporal Range query job.
 *
 * @author Wang Yue
 */
public class TrajectorySTRangeQueryJob extends QueryJob {
  private BoundingBox bbox;
  private LinkedTransferQueue<TrajectoryPoint> resultQueue;
  private TimeValue startTime;
  private TimeValue endTime;
  private String schema;
  private List<String> idList;

  public TrajectorySTRangeQueryJob(Table table, List<ByteArrayRange> ranges, int start, int end,
                                   BoundingBox bbox, LinkedTransferQueue<TrajectoryPoint> resultQueue,
                                   TimeValue startTime, TimeValue endTime, String schema, List<String> idList) {
    super(table, ranges, start, end);
    this.bbox = bbox;
    this.resultQueue = resultQueue;
    this.startTime = startTime;
    this.endTime = endTime;
    this.schema = schema;
    this.idList = idList;
  }

  @Override
  public void run() {
    AvroTrajectoryPoint avroTrajectoryPoint = new AvroTrajectoryPoint(schema);

    for (int i = start; i < end; i++) {
      Scan scan = new Scan();
      ByteArrayRange range = ranges.get(i);
      if (range.getStart().equals(range.getEnd())) {
        scan.withStartRow(range.getStart().getBytes(), true);
        scan.setFilter(new PrefixFilter(range.getStart().getBytes()));
      } else {
        scan.withStartRow(range.getStart().getBytes(), true);
        scan.withStopRow(range.getEnd().getBytes(), true);
      }
      try {
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
          Cell[] cells = result.rawCells();
          ByteArray rowkey = new ByteArray(result.getRow());
          // rowKey filter
          // Parse rowKey to get the time and spatial information.
          // The information length in rowKey is as follows:
          // coarse time: 0~4, precise time:9~12, spatial code(precise: 20): 4~9, id: 12~44
          String temp = new TemporalByteArray(rowkey.slice(0, 4).getBytes()).toString()
                  + new TemporalByteArray(rowkey.slice(9, 12).getBytes()).toString();
          TimeValue tempTime = new TimeValue(
                  Integer.parseInt(temp.substring(0, 4)),
                  Integer.parseInt(temp.substring(4, 6)),
                  Integer.parseInt(temp.substring(6, 8)),
                  Integer.parseInt(temp.substring(8, 10)),
                  Integer.parseInt(temp.substring(10, 12)),
                  Integer.parseInt(temp.substring(12, 14)));
          // time filter
          if (tempTime.compareTo(startTime) >= 0 && tempTime.compareTo(endTime) <= 0) {
            for (Cell cell : cells) {
              TrajectoryPoint point = avroTrajectoryPoint.deserialize(CellUtil.cloneValue(cell));
              // spatial filter
              if (point.getLat() <= bbox.getMaxLat() && point.getLat() >= bbox.getMinLat()
                      && point.getLng() <= bbox.getMaxLon() && point.getLng() >= bbox.getMinLon()) {
                // id filter
                if (!idList.isEmpty()) {
                  String id = new String(rowkey.slice(12, 44).getBytes());
                  if (idList.contains(id)) {
                    resultQueue.add(point);
                  }
                } else {
                  resultQueue.add(point);
                }
              }
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    // add end mark for this job
    resultQueue.add(new NullPoint());
  }
}

