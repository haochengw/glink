package com.github.tm.glink.hbase.query;

import com.github.tm.glink.hbase.geodatabase.DataTypeEnum;
import com.github.tm.glink.hbase.geodatabase.IndexTable;
import com.github.tm.glink.hbase.query.queryCondition.QueryCondition;

import java.util.List;

public class STRangeQuery extends QueryOptions{
  public STRangeQuery() {}

  public STRangeQuery(final DataTypeEnum dataTypeEnum, final IndexTable indexTable) {

  }

  @Override
  public <T> List<T> executeQuery(QueryCondition queryCondition) throws Throwable {
    return null;
  }
}
