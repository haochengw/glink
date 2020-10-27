package com.github.tm.glink.hbase.query.queryCondition;

import java.time.LocalDateTime;

/**
 *
 * @author Wang Yue
 */
public class IDTemporalCondition extends QueryCondition {
  private String id;
  private LocalDateTime startDate = null;
  private LocalDateTime endDate = null;
  private LocalDateTime startTime = null;
  private LocalDateTime endTime = null;
  private String[] attributeList;
  private boolean current = false;

  private IDTemporalCondition(String id, String[] attributeList) {
    this.id = id;
    this.attributeList = attributeList;
  }

  public IDTemporalCondition(String id, LocalDateTime startDate, String[] attributeList) {
    this.id = id;
    this.startDate = startDate;
    this.attributeList = attributeList;
    this.current = true;
  }

  public IDTemporalCondition(String id, LocalDateTime startDate, LocalDateTime endDate, String[] attributeList) {
    this.id = id;
    this.startDate = startDate;
    this.endDate = endDate;
    this.attributeList = attributeList;
  }

  public IDTemporalCondition(String id, LocalDateTime startDate, LocalDateTime endDate, LocalDateTime startTime, LocalDateTime endTime, String[] attributeList) {
    this.id = id;
    this.startDate = startDate;
    this.endDate = endDate;
    this.startTime = startTime;
    this.endTime = endTime;
    this.attributeList = attributeList;
  }


  public static IDTemporalCondition idWithAttribute(String id,
                                                    String[] attributeList) {
    return new IDTemporalCondition(id, attributeList);
  }

  public static IDTemporalCondition idWithAttributefromStartToNow(String id,
                                                                  LocalDateTime startDate,
                                                                  String[] attributeList) {
    return new IDTemporalCondition(id, startDate, attributeList);
  }

  public static IDTemporalCondition idWithAttributefromContinuousTime(String id,
                                                                      LocalDateTime startDate,
                                                                      LocalDateTime endDate,
                                                                      String[] attributeList) {
    return new IDTemporalCondition(id, startDate, endDate, attributeList);
  }

  public static IDTemporalCondition idWithAttributefromDiscreteTime(String id,
                                                                    LocalDateTime startDate,
                                                                    LocalDateTime endDate,
                                                                    LocalDateTime startTime,
                                                                    LocalDateTime endTime,
                                                                    String[] attributeList) {
    return new IDTemporalCondition(id, startDate, endDate, startTime, endTime, attributeList);
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
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
}
