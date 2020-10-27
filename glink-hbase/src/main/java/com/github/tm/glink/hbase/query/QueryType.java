package com.github.tm.glink.hbase.query;

/**
 * This class enumerate all the query types.
 *
 * @author Wang Yue
 * */
public enum QueryType {
  IDTemporal_QUERY(0, "IDTemporal_QUERY"),
  IDSTRANGE_QUERY(1, "IDSTRANGE_QUERY"),
  STRANGE_QUERY(2, "STRANGE_QUERY"),
  KNN_QUERY(3, "KNN_QUERY");


  private int id;
  private String name;

  QueryType(int id, String name) {
    this.id = id;
    this.name = name;
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }
}
