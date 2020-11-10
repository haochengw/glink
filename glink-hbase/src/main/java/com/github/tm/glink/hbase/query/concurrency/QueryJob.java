package com.github.tm.glink.hbase.query.concurrency;


import com.github.tm.glink.features.Point;
import com.github.tm.stindex.ByteArrayRange;
import org.apache.hadoop.hbase.client.Table;

import java.util.List;
import java.util.concurrent.LinkedTransferQueue;

/**
 * Abstract query job.
 * @author Wang Yue
 */
public abstract class QueryJob implements Runnable {
  protected Table table;
  protected List<ByteArrayRange> ranges;
  protected int start;
  protected int end;

  public QueryJob(Table table, List<ByteArrayRange> ranges, int start, int end) {
    this.table = table;
    this.ranges = ranges;
    this.start = start;
    this.end = end;
  }

  @Override
  public void run() {
  }
}
