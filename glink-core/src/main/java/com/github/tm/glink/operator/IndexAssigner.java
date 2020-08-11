package com.github.tm.glink.operator;

import com.github.tm.glink.fearures.Point;
import com.github.tm.glink.index.GridIndex;
import com.github.tm.glink.index.H3Index;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * @author Yu Liebing
 */
public class IndexAssigner extends RichMapFunction<Point, Point> {

  private int res;
  private transient GridIndex gridIndex;

  public IndexAssigner(int res) {
    this.res = res;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    gridIndex = new H3Index(res);
  }

  @Override
  public Point map(Point point) throws Exception {
    point.setIndex(gridIndex.getIndex(point.getLat(), point.getLng()));
    return point;
  }
}
