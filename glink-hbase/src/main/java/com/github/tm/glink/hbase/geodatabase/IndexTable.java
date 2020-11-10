package com.github.tm.glink.hbase.geodatabase;

import com.github.tm.glink.hbase.query.*;
import org.apache.hadoop.hbase.client.Table;

/**
 * This class represents a index table in HBase of a data set.
 *
 * @author Wang Yue
 */
public class IndexTable {
  private Table table;

  public IndexTable(Table table) {
    this.table = table;
  }

  public Table getTable() {
    return table;
  }

  /**
   * @param queryType query type need to executed
   * @param dataType data type of this dataset
   * */
  public QueryOptions getQuery(QueryType queryType, DataTypeEnum dataType) {
    switch (queryType) {
      case IDTemporal_QUERY:
        return new IDTemporalQuery(dataType, this);
      case STRANGE_QUERY:
        return new STRangeQuery(dataType, this);
      case KNN_QUERY:
        return new KNNQuery(dataType, this);
      default:
        throw new UnsupportedOperationException("Unsupported query type");
    }
  }
}
