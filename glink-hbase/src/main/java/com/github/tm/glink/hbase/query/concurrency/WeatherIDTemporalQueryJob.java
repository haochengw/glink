package com.github.tm.glink.hbase.query.concurrency;

import com.alibaba.fastjson.JSONObject;
import com.github.tm.stindex.ByteArray;
import com.github.tm.stindex.ByteArrayRange;
import com.github.tm.stindex.TemporalByteArray;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.concurrent.LinkedTransferQueue;

/**
 * Weather ID Temporal query job.
 * @author Wang Yue
 */
public class WeatherIDTemporalQueryJob extends QueryJob {
  private String[] attributeList;
  private LinkedTransferQueue<JSONObject> resultQueue;

  public WeatherIDTemporalQueryJob(Table table, List<ByteArrayRange> ranges, int start,
                                      int end, LinkedTransferQueue<JSONObject> resultQueue,
                                      String[] attributeList) {
    super(table, ranges, start, end);
    this.attributeList = attributeList;
    this.resultQueue = resultQueue;
  }

  @Override
  public void run() {
    for (int i = start; i < end; i++) {
      Scan scan = new Scan();
      for (String s : attributeList) {
        if ("allTypes".equals(s)) {
          break;
        }
        scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes(s));
      }
      if (ranges.get(i).isSingleValue()) {
        scan.withStartRow(ranges.get(i).getStart().getBytes(), true);
        scan.setFilter(new PrefixFilter(ranges.get(i).getStart().getBytes()));
      } else {
        scan.withStartRow(ranges.get(i).getStart().getBytes(), true);
        scan.withStopRow(ranges.get(i).getEnd().getBytes(), true);
      }
      try {
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
          Cell[] cells = result.rawCells();
          ByteArray rowkey = new ByteArray(result.getRow());
          JSONObject object = new JSONObject();
          object.put("ID", new String(rowkey.slice(0, 5).getBytes()));
          object.put("Time", new TemporalByteArray(rowkey.slice(5, 10).getBytes()).toString());
          for (Cell cell : cells) {
            object.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
          }
          resultQueue.add(object);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    // add end mark for this job
    resultQueue.add(new JSONObject());
  }
}

