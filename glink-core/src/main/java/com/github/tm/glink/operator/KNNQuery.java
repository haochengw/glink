package com.github.tm.glink.operator;

import com.github.tm.glink.feature.Point;
import com.github.tm.glink.feature.Coordinate;
import com.github.tm.glink.operator.judgement.IndexKNNJudgement;
import com.github.tm.glink.operator.judgement.NativeKNNJudgement;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author Yu Liebing
 */
public class KNNQuery {

  public static DataStream<Point> pointKNNQuery(
          DataStream<Point> geoDataStream,
          Coordinate queryPoint,
          int k,
          int windowSize,
          boolean useIndex,
          int indexRes) {
    int partitionNum = 2;
    if (useIndex) {
      return geoDataStream.map(new IndexAssigner(indexRes))
              .keyBy(r -> Math.abs(r.getId().hashCode() % partitionNum))
              .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
              .apply(new IndexKNNJudgement.IndexKeyedKNNJudgement(queryPoint, k, indexRes))
              .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
              .apply(new NativeKNNJudgement.NativeAllKNNJudgement(queryPoint, k));
    }
    return geoDataStream.keyBy(r -> Math.abs(r.getId().hashCode() % partitionNum))
            .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
            .apply(new NativeKNNJudgement.NativeKeyedKNNJudgement(queryPoint, k))
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
            .apply(new NativeKNNJudgement.NativeAllKNNJudgement(queryPoint, k));
  }
}
