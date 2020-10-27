package com.github.tm.glink.hbase.query;

import com.github.tm.glink.hbase.geodatabase.DataTypeEnum;
import com.github.tm.glink.hbase.geodatabase.IndexTable;
import com.github.tm.glink.hbase.query.queryCondition.QueryCondition;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * This class is the abstract class for query action.
 * @author Wang Yue
 */
public abstract class QueryOptions {
  protected DataTypeEnum dataTypeEnum;
  protected IndexTable indexTable;
//  protected Coding coding;

  /**
   * for multi-thread supporting.
   * */
  protected final int threadNum;
  protected final ThreadPoolExecutor queryWorkersPool;
//  protected final LinkedTransferQueue<SerializeGeometry> resultQueue;

  public QueryOptions() {
    threadNum = 0;
    queryWorkersPool = null;
//    resultQueue = null;
  }

  public QueryOptions(DataTypeEnum dataTypeEnum, IndexTable indexTable) {
    this.dataTypeEnum = dataTypeEnum;
    this.indexTable = indexTable;

    threadNum = Runtime.getRuntime().availableProcessors() + 1;
//        threadNum = 1;
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(true).setNameFormat("internal-pol-%d").build();
    queryWorkersPool =
            (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum, threadFactory);
    queryWorkersPool.prestartAllCoreThreads();
//    resultQueue = new LinkedTransferQueue<>();
  }

  public abstract <T> List<T> executeQuery(QueryCondition queryCondition) throws Throwable;
}
