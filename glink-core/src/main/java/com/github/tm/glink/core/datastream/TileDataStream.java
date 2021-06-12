package com.github.tm.glink.core.datastream;

import com.github.tm.glink.core.enums.TileAggregateType;
import com.github.tm.glink.core.tile.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Point;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;


/**
 * @param <T1> Any class extends {@link Point}.
 * @param <T2> The ACC type of {@link AggregateFunction}.
 */
public class TileDataStream<T1 extends Point, T2> {

  private DataStream<TileResult> tileResultDataStream;

  /**
   *
   * @param aggFieldIndex The index of the field in the user data to be aggregated. If the aggregateType is COUNT, this parameter is useless.
   */
  public TileDataStream(
          SpatialDataStream<T1> pointDataStream,
          TileAggregateType aggregateType,
          WindowAssigner windowAssigner,
          int aggFieldIndex,
          int hLevel,
          int lLevel) {
    tileResultDataStream = pointDataStream.getDataStream()
            .flatMap(new PixelGenerateFlatMap(hLevel, lLevel))
            .keyBy(new DefaultKeyByTile())
            .window(windowAssigner)
            .aggregate(TileAggregateType.getAggregateFunction(aggregateType, aggFieldIndex), new AddWindowTimeInfo());
  }

  // TODO: Two phase aggregate.
  public TileDataStream(
          SpatialDataStream<T1> pointDataStream,
          AggregateFunction<Tuple2<PixelResult, T1>, Map<Pixel, T2>, TileResult> aggregateFunction,
          WindowAssigner windowAssigner,
          int hLevel,
          int lLevel,
          Boolean addSalt,
          AggregateFunction<TileResult, TileResult, TileResult> finalTileAggregate) {
    if (addSalt) {
      tileResultDataStream = pointDataStream.getDataStream()
              .flatMap(new PixelGenerateFlatMap(hLevel, lLevel))
              .keyBy(new KeyByTileWithSalt())
              // 预聚合
              .window(windowAssigner)
              .aggregate(aggregateFunction, new AddWindowTimeInfo())
              // final aggregate
              .keyBy(new KeyByTileWithOutSalt())
              .window(windowAssigner)
              .aggregate(finalTileAggregate);
    } else {
      tileResultDataStream = pointDataStream.getDataStream()
              .flatMap(new PixelGenerateFlatMap(hLevel, lLevel))
              .keyBy(new DefaultKeyByTile())
              // 预聚合
              .window(windowAssigner)
              .aggregate(aggregateFunction, new AddWindowTimeInfo());
    }
  }

  public DataStream<TileResult> getTileResultDataStream() {
    return tileResultDataStream;
  }

  private class PixelGenerateFlatMap extends RichFlatMapFunction<T1, Tuple2<PixelResult<Integer>, T1>> {
    private static final long serialVersionUID = 5235060756502253407L;
    private transient TileGrid[] tileGrids;
    int hLevel, lLevel, levelNum;

    PixelGenerateFlatMap(int hLevel, int lLevel) {
      this.hLevel = hLevel;
      this.lLevel = lLevel;
      levelNum = hLevel - lLevel + 1;
    }

    @Override
    public void open(Configuration conf) {
      int length = hLevel - lLevel + 1;
      tileGrids = new TileGrid[length];
      int i = length;
      int j = hLevel;
      while (0 < i) {
        tileGrids[i - 1] = new TileGrid(j);
        i--;
        j--;
      }
    }

    @Override
    public void flatMap(T1 value, Collector<Tuple2<PixelResult<Integer>, T1>> out) throws Exception {
      int i = levelNum;
      while (0 < i) {
        out.collect(new Tuple2<>(new PixelResult<>(tileGrids[i - 1].getPixel(value.getY(), value.getX()), 1), value));
        i = i - 1;
      }
    }
  }

  private class DefaultKeyByTile implements KeySelector<Tuple2<PixelResult<Integer>, T1>, Long> {
    private static final long serialVersionUID = 406340347008662020L;
    @Override
    public Long getKey(Tuple2<PixelResult<Integer>, T1> value) throws Exception {
      return value.f0.getPixel().getTile().toLong();
    }
  }

  private class KeyByTileWithOutSalt implements KeySelector<TileResult, Tile> {
    private static final long serialVersionUID = 406340347008662020L;
    @Override
    public Tile getKey(TileResult value) throws Exception {
      return value.getTile();
    }
  }

  private class KeyByTileWithSalt implements KeySelector<Tuple2<PixelResult<Integer>, T1>, Long> {
    private static final long serialVersionUID = 406340347008662020L;
    private int parallism;
    @Override
    public Long getKey(Tuple2<PixelResult<Integer>, T1> value) throws Exception {
      return value.f0.getPixel().getTile().toLong() + ThreadLocalRandom.current().nextLong(1, 8);
    }
  }

  private static class AddWindowTimeInfo extends ProcessWindowFunction<TileResult, TileResult, Tile, TimeWindow> {

    private static final long serialVersionUID = -1308201162807418668L;

    @Override
    public void process(Tile tile, Context context, Iterable<TileResult> elements, Collector<TileResult> out) throws Exception {
      TileResult tileResult = elements.iterator().next();
      tileResult.setTimeStart(new Timestamp(context.window().getStart()));
      tileResult.setTimeEnd(new Timestamp(context.window().getEnd()));
      out.collect(tileResult);
    }
  }
}
