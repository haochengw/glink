package com.github.tm.glink.examples.demo.xiamen;

import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParamFactory;
import com.github.tm.glink.connector.geomesa.sink.GeoMesaSinkFunction;
import com.github.tm.glink.connector.geomesa.sink.PointToSimpleFeatureConverter;
import com.github.tm.glink.connector.geomesa.source.GeoMesaSourceFunction;
import com.github.tm.glink.connector.geomesa.source.SimpleFeatureToGeometryConverter;
import com.github.tm.glink.connector.geomesa.util.GeoMesaStreamTableSchema;
import com.github.tm.glink.connector.geomesa.util.GeoMesaType;
import com.github.tm.glink.core.datastream.BroadcastSpatialDataStream;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.datastream.TileDataStream;
import com.github.tm.glink.core.enums.GeometryType;
import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.core.enums.TileAggregateType;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.core.tile.Pixel;
import com.github.tm.glink.core.tile.PixelResult;
import com.github.tm.glink.core.tile.TileResult;
import com.github.tm.glink.examples.utils.HBaseCatalogCleaner;
import com.github.tm.glink.sql.util.Schema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.time.Duration;
import java.util.*;

public class XiamenTwoJobs {

  // For spatial data stream source.
  public static final String ZOOKEEPERS = "u0:2181,u1:2181,u2:2181";
  public static final String KAFKA_BOOSTRAP_SERVERS = "u0:9092";
  public static final String KAFKA_GROUP_ID = "TWOJOBS";
  public static final String CATALOG_NAME = "Xiamen";
  public static final String TILE_SCHEMA_NAME = "Heatmap";
  public static final String POINTS_SCHEMA_NAME = "JoinedPoints";
  public static final long WIN_LEN = 5L;
  public static final int PARALLELISM = 20;
  public static final int CARNO_FIELD_UDINDEX = 4;
  public static final int TIMEFIELDINDEX = 3;
  public static final TextFileSplitter SPLITTER = TextFileSplitter.CSV;
  // For heatmap generation.
  public static final int H_LEVEL = 18;
  public static final int L_LEVEL = 12;

  public static void main(String[] args) throws Exception {
    Time windowLength = Time.minutes(WIN_LEN);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);
    env.setParallelism(PARALLELISM);
    env.disableOperatorChaining();

    // Get heatmap sink function
    Configuration confForTiles = new Configuration();
    confForTiles.setString("geomesa.data.store", "hbase");
    confForTiles.setString("hbase.catalog", CATALOG_NAME);
    confForTiles.setString("geomesa.schema.name", TILE_SCHEMA_NAME);
    confForTiles.setString("hbase.zookeepers", ZOOKEEPERS);
    confForTiles.setString("geomesa.primary.field.name", "id");
    confForTiles.setString("geomesa.indices.enabled", "id");
    List<Tuple2<String, GeoMesaType>> fieldNamesToTypesForTile = new LinkedList<>();
    fieldNamesToTypesForTile.add(new Tuple2<>("pk", GeoMesaType.STRING));
    fieldNamesToTypesForTile.add(new Tuple2<>("tile_id", GeoMesaType.LONG));
    fieldNamesToTypesForTile.add(new Tuple2<>("windowEndTime", GeoMesaType.DATE));
    fieldNamesToTypesForTile.add(new Tuple2<>("tile_result", GeoMesaType.STRING));
    GeoMesaDataStoreParam tileDataStoreParam = GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam("HBase");
    tileDataStoreParam.initFromConfigOptions(confForTiles);
    GeoMesaStreamTableSchema heatMapSchema = new GeoMesaStreamTableSchema(fieldNamesToTypesForTile, confForTiles);
    GeoMesaSinkFunction heatMapSink = new GeoMesaSinkFunction(tileDataStoreParam, heatMapSchema, new AvroStringTileResultToSimpleFeatureConverter(heatMapSchema));

    // Get polygon source function
    Configuration confForPolygon = new Configuration();
    confForPolygon.setString("geomesa.schema.name", "Geofence");
    confForPolygon.setString("geomesa.spatial.fields", "geom:Polygon");
    confForPolygon.setString("hbase.zookeepers", ZOOKEEPERS);
    confForPolygon.setString("geomesa.data.store", "hbase");
    confForPolygon.setString("hbase.catalog", CATALOG_NAME);
    confForPolygon.setString("geomesa.primary.field.name", "pid");
    List<Tuple2<String, GeoMesaType>> fieldNamesToTypesForPolygon = new LinkedList<>();
    fieldNamesToTypesForPolygon.add(new Tuple2<>("id", GeoMesaType.STRING));
    fieldNamesToTypesForPolygon.add(new Tuple2<>("dtg", GeoMesaType.DATE));
    fieldNamesToTypesForPolygon.add(new Tuple2<>("geom", GeoMesaType.POLYGON));
    fieldNamesToTypesForPolygon.add(new Tuple2<>("name", GeoMesaType.STRING));
    GeoMesaDataStoreParam polygonDataStoreParam = GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam("HBase");
    polygonDataStoreParam.initFromConfigOptions(confForPolygon);
    GeoMesaStreamTableSchema polygonSchema = new GeoMesaStreamTableSchema(fieldNamesToTypesForPolygon, confForPolygon);
    GeoMesaSourceFunction polygonSource = new GeoMesaSourceFunction(polygonDataStoreParam, polygonSchema, new SimpleFeatureToGeometryConverter(polygonSchema));
    SpatialDataStream<Polygon> polygonsds = new SpatialDataStream<Polygon>(env, polygonSource, GeometryType.POLYGON);
    BroadcastSpatialDataStream bsd = new BroadcastSpatialDataStream(polygonsds);

    // Get point sink function
    Configuration confForOutputPoints = new Configuration();
    confForOutputPoints.setString("geomesa.schema.name", POINTS_SCHEMA_NAME);
    confForOutputPoints.setString("geomesa.spatial.fields", "point2:Point");
    confForOutputPoints.setString("hbase.zookeepers", ZOOKEEPERS);
    confForOutputPoints.setString("geomesa.data.store", "hbase");
    confForOutputPoints.setString("hbase.catalog", CATALOG_NAME);
    confForOutputPoints.setString("geomesa.primary.field.name", "pid");
    List<Tuple2<String, GeoMesaType>> fieldNamesToTypesForPoints = new LinkedList<>();
    fieldNamesToTypesForPoints.add(new Tuple2<>("point2", GeoMesaType.POINT));
    fieldNamesToTypesForPoints.add(new Tuple2<>("status", GeoMesaType.INTEGER));
    fieldNamesToTypesForPoints.add(new Tuple2<>("speed", GeoMesaType.DOUBLE));
    fieldNamesToTypesForPoints.add(new Tuple2<>("azimuth", GeoMesaType.INTEGER));
    fieldNamesToTypesForPoints.add(new Tuple2<>("ts", GeoMesaType.DATE));
    fieldNamesToTypesForPoints.add(new Tuple2<>("tid", GeoMesaType.STRING));
    fieldNamesToTypesForPoints.add(new Tuple2<>("pid", GeoMesaType.STRING));
    fieldNamesToTypesForPoints.add(new Tuple2<>("fid", GeoMesaType.STRING));
    GeoMesaDataStoreParam pointDataStoreParam = GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam("HBase");
    pointDataStoreParam.initFromConfigOptions(confForOutputPoints);
    GeoMesaStreamTableSchema pointSchema = new GeoMesaStreamTableSchema(fieldNamesToTypesForPoints, confForOutputPoints);
    GeoMesaSinkFunction pointSink = new GeoMesaSinkFunction<>(pointDataStoreParam, pointSchema, new PointToSimpleFeatureConverter(pointSchema));
    // Kafka properties
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", KAFKA_BOOSTRAP_SERVERS);
    props.put("zookeeper.connect", ZOOKEEPERS);
    props.setProperty("group.id",KAFKA_GROUP_ID);
    // 模拟流
    SpatialDataStream<Point> originalDataStream = new SpatialDataStream<Point>(
            env, new FlinkKafkaConsumer<>(KafkaDataProducer.TOPICID, new SimpleStringSchema(), props).setStartFromLatest(),
            4, 5, TextFileSplitter.CSV, GeometryType.POINT, true,
            Schema.types(Integer.class, Double.class, Integer.class, Long.class, String.class, String.class))
            .assignTimestampsAndWatermarks((WatermarkStrategy.<Point>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, timestamp) -> ((Tuple) event.getUserData()).getField(TIMEFIELDINDEX))));
    // 热力图生成
    TileDataStream tileDataStream = new TileDataStream(
            originalDataStream,
            new XiamenHeatmap.CountAggregator(),
            SlidingEventTimeWindows.of(windowLength, Time.minutes(5)),
            H_LEVEL,
            L_LEVEL,
            false,
            TileAggregateType.getFinalAggregateFunction(TileAggregateType.SUM));
    tileDataStream.getTileResultDataStream().addSink(heatMapSink);
    originalDataStream.spatialDimensionJoin(bsd, TopologyType.N_CONTAINS, new XiamenHeatmap.AddFenceId(), new TypeHint<Point>() { })
            .addSink(pointSink);

    env.execute();

  }
}
