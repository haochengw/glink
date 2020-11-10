package com.github.tm.glink.hbase.query.queryCondition;

import com.github.tm.stindex.api.BoundingBox;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Spatial temporal range query condition.
 *
 * @author Wang Yue
 * */
public class STRangeCondition extends QueryCondition {
  private BoundingBox boundingBox;
  private LocalDateTime startDate;
  private LocalDateTime endDate;
  private LocalDateTime startTime;
  private LocalDateTime endTime;
  private String[] attributeList;
  private List<String> idList;
  private boolean current = false;

  public STRangeCondition(BoundingBox boundingBox) {
    this.boundingBox = boundingBox;
  }

  public STRangeCondition(BoundingBox boundingBox, LocalDateTime startDate, String[] attributeList, List<String> idList) {
    this.boundingBox = boundingBox;
    this.startDate = startDate;
    this.attributeList = attributeList;
    this.current = true;
    this.idList = idList;
  }

  public STRangeCondition(BoundingBox boundingBox, LocalDateTime startDate, LocalDateTime endDate, String[] attributeList, List<String> idList) {
    this.boundingBox = boundingBox;
    this.startDate = startDate;
    this.endDate = endDate;
    this.attributeList = attributeList;
    this.idList = idList;
  }

  public STRangeCondition(BoundingBox boundingBox, LocalDateTime startDate, LocalDateTime endDate, LocalDateTime startTime, LocalDateTime endTime, String[] attributeList, List<String> idList) {
    this.boundingBox = boundingBox;
    this.startDate = startDate;
    this.endDate = endDate;
    this.startTime = startTime;
    this.endTime = endTime;
    this.attributeList = attributeList;
    this.idList = idList;
  }

  public static STRangeCondition spatialInfoIndex(BoundingBox boundingBox) {
    return new STRangeCondition(boundingBox);
  }

  public static STRangeCondition toCurrentWithAttribute(BoundingBox boundingBox,
                                                        LocalDateTime startDate,
                                                        String[] attributeList,
                                                        List<String> idList) {
    return new STRangeCondition(boundingBox, startDate, attributeList, idList);
  }

  public static STRangeCondition fromContinuousTimeWithAttribute(BoundingBox boundingBox,
                                                                 LocalDateTime startDate,
                                                                 LocalDateTime endDate,
                                                                 String[] attributeList,
                                                                 List<String> idList) {
    return new STRangeCondition(boundingBox, startDate, endDate, attributeList, idList);
  }

  public static STRangeCondition fromDiscreteTimeWithAttribute(BoundingBox boundingBox,
                                                               LocalDateTime startDate,
                                                               LocalDateTime endDate,
                                                               LocalDateTime startTime,
                                                               LocalDateTime endTime,
                                                               String[] attributeList,
                                                               List<String> idList) {
    return new STRangeCondition(boundingBox, startDate, endDate, startTime, endTime, attributeList, idList);
  }


  public BoundingBox getBoundingBox() {
    return boundingBox;
  }

  public void setBoundingBox(BoundingBox boundingBox) {
    this.boundingBox = boundingBox;
  }

  public LocalDateTime getStartDate() {
    return startDate;
  }

  public void setStartDate(LocalDateTime startDate) {
    this.startDate = startDate;
  }

  public LocalDateTime getEndDate() {
    return endDate;
  }

  public void setEndDate(LocalDateTime endDate) {
    this.endDate = endDate;
  }

  public LocalDateTime getStartTime() {
    return startTime;
  }

  public void setStartTime(LocalDateTime startTime) {
    this.startTime = startTime;
  }

  public LocalDateTime getEndTime() {
    return endTime;
  }

  public void setEndTime(LocalDateTime endTime) {
    this.endTime = endTime;
  }

  public String[] getAttributeList() {
    return attributeList;
  }

  public void setAttributeList(String[] attributeList) {
    this.attributeList = attributeList;
  }

  public boolean isCurrent() {
    return current;
  }

  public void setCurrent(boolean current) {
    this.current = current;
  }

  public List<String> getIdList() {
    return idList;
  }

  public void setIdList(List<String> idList) {
    this.idList = idList;
  }
}
