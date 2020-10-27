package com.github.tm.glink.hbase.geodatabase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

/**
 * This class gives the connection to HBase, and encapsulate
 * the management of Namespace and table API for HBase.
 * This class is singleton, and could only exist one instance.
 *
 * GeoDataBase Guarantee concurrency safe.
 *
 * @author Yu Liebing
 * Create on 2018-12-29.
 */
public class GeoDatabase {
  // The only instance. Must be initialized by static field to support concurrency.
  private static GeoDatabase instance = new GeoDatabase();

  private static Configuration configuration;
  private static Connection connection;
  private static Admin admin;

  // Must be private.
  private GeoDatabase() {
    configuration = HBaseConfiguration.create();
  }

  public static GeoDatabase getInstance() {
    return instance;
  }

  /**
   * Configuration
   * */
  public void addConfigurationResource(String path) {
    configuration.addResource(new Path(path));
  }

  public void setConfiguration(String key, String value) {
    configuration.set(key, value);
  }

  public String getConfiguration(String key) {
    return configuration.get(key);
  }

  /**
   * Sub database operations.
   * */
  public List<String> listSubDatabase() throws IOException {
    List<String> res = new ArrayList<>();
    NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
    for (NamespaceDescriptor nsd: namespaceDescriptors) {
      res.add(nsd.getName());
    }
    return res;
  }

  public boolean subDatabaseExists(String subDatabaseName) throws IOException {
    List<String> subDataBaseNames = this.listSubDatabase();
    for (String name : subDataBaseNames) {
      if (name.equals(subDatabaseName)) {
        return true;
      }
    }
    return false;
  }

  public void createSubDatabase(String subDatabaseName) throws IOException {
    if (this.subDatabaseExists(subDatabaseName)) {
      throw new IOException("Sub database already exists.");
    }
    NamespaceDescriptor namespaceDescriptor =
            NamespaceDescriptor.create(subDatabaseName).build();
    admin.createNamespace(namespaceDescriptor);
  }

  public void deleteSubDatabase(String subDatabaseName) throws IOException {
    if (!this.subDatabaseExists(subDatabaseName)) {
      throw new IOException("Sub database does not exists.");
    }
    admin.deleteNamespace(subDatabaseName);
  }

  /**
   * Connection
   * */
  public void closeConnection() {
    try {
      if (admin != null) {
        admin.close();
        admin = null;
      }
      if (null != connection) {
        connection.close();
        connection = null;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void openConnection() {
    int threads = Runtime.getRuntime().availableProcessors() * 4;
    ExecutorService service = new ForkJoinPool(threads);
    try {
      connection = ConnectionFactory.createConnection(configuration, service);
      admin = connection.getAdmin();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * HBase table options
   * */
  public void createTable(String tableName, String[] columnFamilies) throws IOException {
    if (this.tableExists(tableName)) {
      throw new IOException("Table already exists.");
    } else {
      HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
      for (String str : columnFamilies) {
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
        hTableDescriptor.addFamily(hColumnDescriptor);
      }
      admin.createTable(hTableDescriptor);
    }
  }

  public boolean tableExists(String tableName) throws IOException {
    return admin.tableExists(TableName.valueOf(tableName));
  }

  public void deleteTable(String tableName) throws IOException {
    if (this.tableExists(tableName)) {
      admin.disableTable(TableName.valueOf(tableName));
      admin.deleteTable(TableName.valueOf(tableName));
    } else {
      throw new IOException("Table does not exists.");
    }
  }

  public Table getTable(String tableName) throws IOException {
    return connection.getTable(TableName.valueOf(tableName));
  }

  public HTableDescriptor[] listTables() throws IOException {
    HTableDescriptor[] hTableDescriptors = admin.listTables();
//    for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
//      System.out.println(hTableDescriptor.getNameAsString());
//    }
    return hTableDescriptors;
  }
}

