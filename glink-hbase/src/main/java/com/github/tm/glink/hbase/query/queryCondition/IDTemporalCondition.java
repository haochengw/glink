package com.github.tm.glink.hbase.query.queryCondition;

import java.time.LocalDateTime;
import java.util.List;

/**
 * ID temporal range query condition.
 *
 * @author Wang Yue
 */
public class IDTemporalCondition extends QueryCondition {
  private List<String> idList;
  private LocalDateTime startDate = null;
  private LocalDateTime endDate = null;
  private LocalDateTime startTime = null;
  private LocalDateTime endTime = null;
  private String[] attributeList;
  private boolean current = false;

  private IDTemporalCondition(List<String> idList, String[] attributeList) {
    this.idList = idList;
    this.attributeList = attributeList;
  }

  public IDTemporalCondition(List<String> idList, LocalDateTime startDate, String[] attributeList) {
    this.idList = idList;
    this.startDate = startDate;
    this.attributeList = attributeList;
    this.current = true;
  }

  public IDTemporalCondition(List<String> idList, LocalDateTime startDate, LocalDateTime endDate, String[] attributeList) {
    this.idList = idList;
    this.startDate = startDate;
    this.endDate = endDate;
    this.attributeList = attributeList;
  }

  public IDTemporalCondition(List<String> idList, LocalDateTime startDate, LocalDateTime endDate, LocalDateTime startTime, LocalDateTime endTime, String[] attributeList) {
    this.idList = idList;
    this.startDate = startDate;
    this.endDate = endDate;
    this.startTime = startTime;
    this.endTime = endTime;
    this.attributeList = attributeList;
  }


  public static IDTemporalCondition idWithAttribute(List<String> idList,
                                                    String[] attributeList) {
    return new IDTemporalCondition(idList, attributeList);
  }

  public static IDTemporalCondition idWithAttributeToCurrent(List<String> idList,
                                                             LocalDateTime startDate,
                                                             String[] attributeList) {
    return new IDTemporalCondition(idList, startDate, attributeList);
  }

  public static IDTemporalCondition idWithAttributeFromContinuousTime(List<String> idList,
                                                                      LocalDateTime startDate,
                                                                      LocalDateTime endDate,
                                                                      String[] attributeList) {
    return new IDTemporalCondition(idList, startDate, endDate, attributeList);
  }

  public static IDTemporalCondition idWithAttributeFromDiscreteTime(List<String> idList,
                                                                    LocalDateTime startDate,
                                                                    LocalDateTime endDate,
                                                                    LocalDateTime startTime,
                                                                    LocalDateTime endTime,
                                                                    String[] attributeList) {
    return new IDTemporalCondition(idList, startDate, endDate, startTime, endTime, attributeList);
  }

  public List<String> getIdList() {
    return idList;
  }

  public void setIdList(List<String> idList) {
    this.idList = idList;
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
