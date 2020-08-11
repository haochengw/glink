package com.github.tm.glink.source;

import com.github.tm.glink.fearures.Point;
import org.apache.flink.configuration.Configuration;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * This class is used to parse point objects from csv files.
 * In each csv file, one line represents a point, and each
 * line contains the following four elements:
 * 1. id, will be parsed as <code>String<code/>
 * 2. lat, will be parsed as <code>double<code/>
 * 3. lng, will be parsed as <code>double</code>
 * 4. time, the format is yyyy-MM-dd HH:mm:ss
 * Each element is separated by a comma.
 *
 * @author Yu Liebing
 */
public class CSVPointSource extends CSVGeoObjectSource<Point> {

  private transient DateTimeFormatter formatter;

  public CSVPointSource(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  }

  @Override
  public Point parseLine(String line) {
    String[] items = line.split(",");
    long timestamp = LocalDateTime.parse(items[3], formatter).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    return new Point(
            items[0],
            Float.parseFloat(items[1]),
            Float.parseFloat(items[2]),
            timestamp);
  }
}
