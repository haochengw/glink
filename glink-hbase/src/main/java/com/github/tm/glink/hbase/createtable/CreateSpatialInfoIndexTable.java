package com.github.tm.glink.hbase.createtable;

import com.github.tm.stindex.ByteArray;
import com.github.tm.stindex.dimension.BasicDimensionDefinition;
import com.github.tm.stindex.spatial.GridIndex;
import com.github.tm.stindex.spatial.sfc.SFCDimensionDefinition;
import com.github.tm.stindex.spatial.sfc.SFCFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * This class is used to build the spatial information index table
 * of meteorological observation data at one time.
 *
 * @author Wang Yue
 */
public class CreateSpatialInfoIndexTable {
  public static Configuration configuration;
  public static Connection connection;
  public static Admin admin;

  public static void main(String[] args) throws IOException {
    init();
    createTable("Hubei_WeatherBase_SpatialInfoIndex", new String[]{"f"});
    Table table = connection.getTable(TableName.valueOf("Hubei_WeatherBase_SpatialInfoIndex"));

    String path = "glink-examples/src/main/resources/WeatherBaseData/SURF_CHN_MUL_HOR_STATION.csv";
    FileReader fr = new FileReader(new File(path));
    BufferedReader reader = new BufferedReader(fr);
    String line;
    while ((line = reader.readLine()) != null) {
      String[] s = line.split(",");
      String location = s[0] + "," + s[2];
      String id = s[1];
      double lat = Double.parseDouble(s[3]) / 100;
      double lng = Double.parseDouble(s[4]) / 100;
      double sensorAltitude = Double.parseDouble(s[5]);
      double obsAltitude = Double.parseDouble(s[6]);

      SFCDimensionDefinition[] dimensions = {
              new SFCDimensionDefinition(new BasicDimensionDefinition(-180.0, 180.0), 20),
              new SFCDimensionDefinition(new BasicDimensionDefinition(-90.0, 90.0), 20)};
      GridIndex zorder = SFCFactory.createSpaceFillingCurve(dimensions, SFCFactory.SFCType.ZORDER);
      double[] lngLat = new double[]{lng, lat};
      ByteArray rowKey = zorder.getIndex(lngLat);

      Put put = new Put(rowKey.getBytes());
      put.addColumn("f".getBytes(), "id".getBytes(), id.getBytes());
      put.addColumn("f".getBytes(), "loc".getBytes(), location.getBytes());
      put.addColumn("f".getBytes(), "lat".getBytes(), Bytes.toBytes(lat));
      put.addColumn("f".getBytes(), "lng".getBytes(), Bytes.toBytes(lng));
      put.addColumn("f".getBytes(), "sa".getBytes(), Bytes.toBytes(sensorAltitude));
      put.addColumn("f".getBytes(), "oa".getBytes(), Bytes.toBytes(obsAltitude));
      table.put(put);
    }
    System.out.println("Data insert success!");
    close();
  }

  public static void init() {
    configuration = HBaseConfiguration.create();
    try {
      connection = ConnectionFactory.createConnection(configuration);
      admin = connection.getAdmin();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void close() {
    try {
      if (admin != null) {
        admin.close();
      }
      if (null != connection) {
        connection.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * create table
   *
   * @param myTableName
   * @param colFamily
   * @throws IOException
   */
  public static void createTable(String myTableName, String[] colFamily) throws IOException {
    TableName tableName = TableName.valueOf(myTableName);

    if (admin.tableExists(tableName)) {
      System.out.println("talbe is exists!");
    } else {
      HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
      for (String str : colFamily) {
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
        hTableDescriptor.addFamily(hColumnDescriptor);
      }
      admin.createTable(hTableDescriptor);
      System.out.println("create table success");
    }
  }
}
