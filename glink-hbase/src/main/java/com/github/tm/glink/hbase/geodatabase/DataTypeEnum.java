package com.github.tm.glink.hbase.geodatabase;

/**
 * This class enumerate data types in the HBase.
 *
 * @author Wang Yue
 * */

public enum DataTypeEnum {
  /**
   * A series of trajectory points.
   */
  TRAJECTORY(0, "XiamenTrajectory"),

  /**
   * Meteorological data.
   */
  WEATHER(1, "WBData");

  // todo:Other types of data to be extended.

  private int index;
  private String name;

  DataTypeEnum(int index, String name) {
    this.index = index;
    this.name = name;
  }

  public int getIndex() {
    return index;
  }

  public String getName() {
    return name;
  }

  public static DataTypeEnum getDataTypeEnum(String name) {
    switch (name) {
      case "XiamenTrajectory":
        return TRAJECTORY;
      case "WeatherBaseData":
        return WEATHER;
      default:
        throw new UnsupportedOperationException("Unsupported Data type name");
    }
  }

  @Override
  public String toString() {
    return "index: " + this.index + ", name: " + this.name;
  }
}

