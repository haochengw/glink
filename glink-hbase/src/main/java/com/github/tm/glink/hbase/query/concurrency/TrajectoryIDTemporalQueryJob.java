package com.github.tm.glink.hbase.query.concurrency;

import com.github.tm.glink.features.NullPoint;
import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.features.avro.AvroTrajectoryPoint;
import com.github.tm.stindex.ByteArray;
import com.github.tm.stindex.ByteArrayRange;
import com.github.tm.stindex.TemporalByteArray;
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
 * Trajectory ID Temporal query job.
 * @author Wang Yue
 */
public class TrajectoryIDTemporalQueryJob extends QueryJob {
  private TimeValue startTime;
  private TimeValue endTime;
  private LinkedTransferQueue<TrajectoryPoint> resultQueue;
  private String schema;

  public TrajectoryIDTemporalQueryJob(Table table, List<ByteArrayRange> ranges, int start,
                                      int end, LinkedTransferQueue<TrajectoryPoint> resultQueue,
                                      TimeValue startTime, TimeValue endTime, String schema) {
    super(table, ranges, start, end);
    this.startTime = startTime;
    this.endTime = endTime;
    this.resultQueue = resultQueue;
    this.schema = schema;
  }

  @Override
  public void run() {
    AvroTrajectoryPoint avroTrajectoryPoint = new AvroTrajectoryPoint(schema);

    for (int i = start; i < end; i++) {
      Scan scan = new Scan();
      if (ranges.get(i).isSingleValue()) {
        scan.withStartRow(ranges.get(i).getStart().getBytes(), true);
        scan.setFilter(new PrefixFilter(ranges.get(i).getStart().getBytes()));
      } else {
        scan.withStartRow(ranges.get(i).getStart().getBytes(), true);
        scan.withStopRow(ranges.get(i).getEnd().getBytes(), true);
      }
      try {
        ResultScanner scanner = table.getScanner(scan);
        // Type2.2: ID Temporal Query.(Minimum time bin: year/month/day)
        if ((startTime == null) || (startTime.getHour() == 0 && startTime.getMinute() == 0 && startTime.getSecond() == 0)) {
          for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
              resultQueue.add(avroTrajectoryPoint.deserialize(CellUtil.cloneValue(cell)));
            }
          }
        } else {
          // Type2.3: ID Temporal Query.(Minimum time bin: hour/minute/second)
          for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            ByteArray rowkey = new ByteArray(result.getRow());
            // Parse rowKey to get the time information.
            String temp = new TemporalByteArray(rowkey.slice(32, 36).getBytes()).toString()
                    + new TemporalByteArray(rowkey.slice(rowkey.length() - 3, rowkey.length()).getBytes()).toString();
            TimeValue tempTime = new TimeValue(
                    Integer.parseInt(temp.substring(0, 4)),
                    Integer.parseInt(temp.substring(4, 6)),
                    Integer.parseInt(temp.substring(6, 8)),
                    Integer.parseInt(temp.substring(8, 10)),
                    Integer.parseInt(temp.substring(10, 12)),
                    Integer.parseInt(temp.substring(12, 14)));
            if (tempTime.compareTo(startTime) >= 0 && tempTime.compareTo(endTime) <= 0) {
              for (Cell cell : cells) {
                resultQueue.add(avroTrajectoryPoint.deserialize(CellUtil.cloneValue(cell)));
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
