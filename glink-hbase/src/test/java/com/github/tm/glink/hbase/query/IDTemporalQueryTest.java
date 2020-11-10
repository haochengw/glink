package com.github.tm.glink.hbase.query;

import com.alibaba.fastjson.JSONObject;
import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.hbase.geodatabase.DataTypeEnum;
import com.github.tm.glink.hbase.geodatabase.GeoDatabase;
import com.github.tm.glink.hbase.geodatabase.IndexTable;
import com.github.tm.glink.hbase.query.queryCondition.IDTemporalCondition;
import com.github.tm.glink.hbase.query.queryCondition.QueryCondition;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class IDTemporalQueryTest {

  private GeoDatabase geoDatabase;
  private static Table table;
  private static IndexTable indexTable;

  @Before
  public void setUp() throws Exception {
    geoDatabase = GeoDatabase.getInstance();
    geoDatabase.openConnection();
  }

  @Test
  public void trajectoryClientQuery() throws Throwable {
    table = geoDatabase.getTable("Xiamen_TrajectoryPoint_IDTST");
    indexTable = new IndexTable(table);
    QueryOptions query = new IDTemporalQuery(DataTypeEnum.TRAJECTORY, indexTable);

    List<String> idList = new LinkedList<>();
    idList.add("0a1c60b6ec3cf05f07479a9e64a3dc90");

    String[] attr = {"speed:double;azimuth:int;status:int"};
    DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    LocalDateTime start = LocalDateTime.parse("2019-06-06 23:00:00", df);
    LocalDateTime end = LocalDateTime.parse("2019-06-06 23:01:00", df);

    QueryCondition condition = IDTemporalCondition.idWithAttribute(idList, attr);
    QueryCondition condition1 = IDTemporalCondition.idWithAttributeToCurrent(idList, start, attr);
    QueryCondition condition2 = IDTemporalCondition.idWithAttributeFromContinuousTime(idList, start, end, attr);

    long s = System.currentTimeMillis();
    List<TrajectoryPoint> results = query.executeQuery(condition2);
    long e = System.currentTimeMillis();
    for (TrajectoryPoint t : results) {
      System.out.println(t);
      Properties attributes = t.getAttributes();
      System.out.println("Speed:" + attributes.get("speed"));
      System.out.println("Azimuth:" + attributes.get("azimuth"));
      System.out.println("Status:" + attributes.get("status"));
    }
    System.out.println("Query takes: " + (e - s) + "ms");
  }

  @Test
  public void weatherClientQuery() throws Throwable {
    table = geoDatabase.getTable("Hubei_WeatherBase_IDT");
    indexTable = new IndexTable(table);
    QueryOptions query = new IDTemporalQuery(DataTypeEnum.WEATHER, indexTable);

    List<String> idList = new LinkedList<>();
    idList.add("57249");

    String[] attr = {"PRS"};
    DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    LocalDateTime start = LocalDateTime.parse("2020-10-09 12:00:00", df);
    LocalDateTime end = LocalDateTime.parse("2020-10-10 08:00:00", df);

    QueryCondition condition = IDTemporalCondition.idWithAttribute(idList, attr);
    QueryCondition condition1 = IDTemporalCondition.idWithAttributeToCurrent(idList, start, attr);
    QueryCondition condition2 = IDTemporalCondition.idWithAttributeFromContinuousTime(idList, start, end, attr);

    long s = System.currentTimeMillis();
    List<JSONObject> results = query.executeQuery(condition);
    long e = System.currentTimeMillis();
    for (JSONObject t : results) {
      System.out.println(t);
    }
    System.out.println("Query takes: " + (e - s) + "ms");
  }
}