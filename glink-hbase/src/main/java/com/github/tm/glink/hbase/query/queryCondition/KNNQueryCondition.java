package com.github.tm.glink.hbase.query.queryCondition;

import com.github.tm.glink.features.Point;
import com.github.tm.stindex.api.BoundingBox;

import java.time.LocalDateTime;
import java.util.Date;

/**
 * KNN query condition,
 * @author Wang Yue
 */

public class KNNQueryCondition extends QueryCondition {
//  private Point queryPoint;
//  private int k;
//  private LocalDateTime startTime;
//  private LocalDateTime endTime;
//  private String[] attributeList;
//  /**
//   * In order to avoid infinite query, the max search distance is required.
//   * The max search distance is 100000m(i.e. 100km) by default.
//   */
//  private double maxSearchDistance = 100000.0;
//  private boolean isCurrent = false;
//
//  public KNNQueryCondition(Point queryPoint, int k, LocalDateTime startTime, LocalDateTime endTime, String[] attributeList) {
//    this.queryPoint = queryPoint;
//    this.k = k;
//    this.startTime = startTime;
//    this.endTime = endTime;
//    this.attributeList = attributeList;
//  }
//
//  public KNNQueryCondition(Point queryPoint, int k, LocalDateTime startTime, LocalDateTime endTime, String[] attributeList, double maxSearchDistance) {
//    this.queryPoint = queryPoint;
//    this.k = k;
//    this.startTime = startTime;
//    this.endTime = endTime;
//    this.attributeList = attributeList;
//    this.maxSearchDistance = maxSearchDistance;
//  }
//
//  public static KNNQueryCondition toCurrentWithAttribute(BoundingBox boundingBox,
//                                                        LocalDateTime startDate,
//                                                        int maxRecuisive,
//                                                        String[] attributeList) {
//    return new STRangeCondition(boundingBox, startDate, maxRecuisive, attributeList);
//  }
//
//
//  public void setMaxSearchDistance(double distance) {
//    this.maxSearchDistance = distance;
//  }
//
//  public double getMaxSearchDistance() {
//    return this.maxSearchDistance;
//  }
//
//  public int getK() {
//    return this.k;
//  }
//
//  public Date getStartTime() {
//    return this.startTime;
//  }
//
//  public Date getEndTime() {
//    return this.endTime;
//  }
//
//  public Point getQueryPoint() {
//    return this.queryPoint;
//  }
}
