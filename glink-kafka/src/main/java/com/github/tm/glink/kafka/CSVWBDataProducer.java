package com.github.tm.glink.kafka;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.avro.AvroPoint;

import java.io.FileNotFoundException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * This class is used to produce point objects with attributes
 * from csv files to Kafka. In each csv file, one line represents
 * an hour's worth of records from one station, and each line contains 16 elements.
 * See the SURF_CHN_MUL_HOR_readme.txt file for details of the corresponding elements.
 * Each element is separated by a comma.
 *
 * @author Wang Yue
 */
public class CSVWBDataProducer extends BaseCSVProducer<String, byte[]> {

  private AvroPoint avroPoint;
  private String[] attrName = {
          "Station_Id_C", "Year", "Mon", "Day", "Hour", "Lat", "Lng", "PRS", "PRS_Sea",
          "WIN_S_Max", "WIN_S_Avg_2mi", "TEM", "RHU", "PRE_1h", "VIS", "tigan"
  };

  public CSVWBDataProducer(String filePath,
                           String serverUrl,
                           int serverPort,
                           String topic,
                           String clientIdConfig,
                           String keySerializer,
                           String valueSerializer,
                           boolean isAsync,
                           CountDownLatch latch) throws FileNotFoundException {
    super(filePath, serverUrl, serverPort, topic, clientIdConfig, keySerializer, valueSerializer, isAsync, latch);
    StringBuilder schema = new StringBuilder();
    for (int i = 7; i < 16; i++) {
      schema.append(attrName[i] + ":string;");
    }
    avroPoint = new AvroPoint(schema.substring(0, schema.length() - 1));
  }

  @Override
  public KeyValue<String, byte[]> parseLine(String line) {
    String[] items = line.split(",");
    if (items.length != attrName.length) {
      return null;
    }

    LocalDateTime localDateTime = LocalDateTime.of(Integer.parseInt(items[1]), Integer.parseInt(items[2]), Integer.parseInt(items[3]), Integer.parseInt(items[4]), 0);
    long timestamp = localDateTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    double lat = Double.parseDouble(items[5]) / 100;
    double lng = Double.parseDouble(items[6]) / 100;
    Point point = new Point(items[0], lat, lng, timestamp);
    Properties attributes = new Properties();
    for (int i = 7; i < 16; i++) {
      attributes.put(attrName[i], items[i]);
    }
    point.setAttributes(attributes);

    return new KeyValue<>(items[0], avroPoint.serialize(point));
  }
}
