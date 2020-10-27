package com.github.tm.glink.hbase.query;

import com.github.tm.stindex.ByteArray;
import com.github.tm.stindex.temporal.ConcatenationTimeEncoding;
import com.github.tm.stindex.temporal.TimeEncoding;
import com.github.tm.stindex.temporal.TimePointDimensionDefinition;
import com.github.tm.stindex.temporal.data.TimeValue;
import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Calendar;

/**
 * Only for test hbase API operations.
 * Delete the class later.
 */
public class ExampleForHbase {
  //对配置信息进行管理的类
  public static Configuration configuration;
  //对连接进行管理的类
  public static Connection connection;
  //对数据库进行管理的类
  public static Admin admin;

  //主函数中的语句请逐句执行，只需删除其前的//即可，如：执行insertRow时请将其他语句注释
  public static void main(String[] args) throws IOException {
    //查看已有表
    //listTables();
    //创建一个表，表名为Score，列族为sname,course
    //createTable("Score", new String[]{"sname", "course"});

    //在Score表中插入一条数据，其行键为95001,sname为Mary（因为sname列族下没有子列所以第四个参数为空）
    //等价命令：put 'Score','95001','sname','Mary'
    //insertRow("Score", "95001", "sname", "", "Mary");
    //在Score表中插入一条数据，其行键为95001,course:Math为88（course为列族，Math为course下的子列）
    //等价命令：put 'Score','95001','score:Math','88'
    //insertRow("Score", "95001", "course", "Math", "88");
    //在Score表中插入一条数据，其行键为95001,course:English为85（course为列族，English为course下的子列）
    //等价命令：put 'Score','95001','score:English','85'
    //insertRow("Score", "95001", "course", "English", "85");

    //1、删除Score表中指定列数据，其行键为95001,列族为course，列为Math
    //执行这句代码前请deleteRow方法的定义中，将删除指定列数据的代码取消注释注释，将删除制定列族的代码注释
    //等价命令：delete 'Score','95001','score:Math'
    //deleteRow("Score", "95001", "course", "Math");

    //2、删除Score表中指定列族数据，其行键为95001,列族为course（95001的Math和English的值都会被删除）
    //执行这句代码前请deleteRow方法的定义中，将删除指定列数据的代码注释，将删除制定列族的代码取消注释
    //等价命令：delete 'Score','95001','score'
    //deleteRow("Score", "95001", "course", "");

    //3、删除Score表中指定行数据，其行键为95001
    //执行这句代码前请deleteRow方法的定义中，将删除指定列数据的代码注释，以及将删除制定列族的代码注释
    //等价命令：deleteall 'Score','95001'
    //deleteRow("Score", "95001", "", "");

    //查询Score表中，行键为95001，列族为course，列为Math的值
    //getData("Score", "95001", "course", "Math");
    //查询Score表中，行键为95001，列族为sname的值（因为sname列族下没有子列所以第四个参数为空）
    //getData("Score", "95001", "sname", "");
    //使用scan扫描weather全表
    scanTable("Hubei_WeatherBase_IDT");

    //删除Score表
    //deleteTable("Score");
  }

  //建立连接,只需要在整个程序的第一个方法的开头使用即可
  public static void init() {
    configuration = HBaseConfiguration.create();
    try {
      connection = ConnectionFactory.createConnection(configuration);
      admin = connection.getAdmin();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  //关闭连接,如果这是最后一个方法，则需要关闭连接。
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
   * 建表。HBase的表中会有一个系统默认的属性作为主键，主键无需自行创建，默认为put命令操作中表名后第一个数据，因此此处无需创建id列
   *
   * @param myTableName 表名
   * @param colFamily   列族名
   * @throws IOException
   */
  public static void createTable(String myTableName, String[] colFamily) throws IOException {

    init();
    TableName tableName = TableName.valueOf(myTableName);  //声明TableName类对象，用于管理表名称
    //判断这个表是否存在
    if (admin.tableExists(tableName)) {
      System.out.println("talbe is exists!");  //存在，输出表已存在
    } else {
      //声明HTableDescriptor类对象，给出表的名称
      //HTableDescriptor类专门用来对表进行细节管理，可告知表中有哪些列族、列等
      HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
      //增加列族
      for (String str : colFamily) {
        //HColumnDescriptor类用于管理列族，对列族中的每一个列都声明HColumnDescriptor类对象
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
        //将HColumnDescriptor对象加到HTableDescriptor对象中
        hTableDescriptor.addFamily(hColumnDescriptor);
        //有了管理表和列族的对象，定义了每个列，就完成了表的创建
      }
      admin.createTable(hTableDescriptor);
      System.out.println("create table success");
    }
    close();
  }

  /**
   * 删除指定表
   *
   * @param tableName 表名
   * @throws IOException
   */
  public static void deleteTable(String tableName) throws IOException {
    init();
    TableName tn = TableName.valueOf(tableName);
    if (admin.tableExists(tn)) {
      admin.disableTable(tn);
      admin.deleteTable(tn);
    }
    close();
  }

  /**
   * 查看已有表
   *
   * @throws IOException
   */
  public static void listTables() throws IOException {
    init();
    HTableDescriptor[] hTableDescriptors = admin.listTables();
    for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
      System.out.println(hTableDescriptor.getNameAsString());
    }
    close();
  }

  /**
   * 向某一行的某一列插入数据
   * 添加数据需要四维定位：表名、行键、列族名、列名(列限定符），最后需要给出对应单元格的数据
   *
   * @param tableName 表名
   * @param rowKey    行键
   * @param colFamily 列族名
   * @param col       列名（如果其列族下没有子列，此参数可为空）
   * @param val       值
   * @throws IOException
   */
  public static void insertRow(String tableName, String rowKey, String colFamily, String col, String val) throws IOException {
    init();
    //声明一个表对象，用于对需要处理的表进行管理
    Table table = connection.getTable(TableName.valueOf(tableName));
    //声明Put对象，用对管理单元格数据的输入
    Put put = new Put(rowKey.getBytes());  //要给出行键
    put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());  //给出列族和列，具体定位到某个单元格
    table.put(put);  //向这个单元格中添加数据
    table.close();   //将这个表关闭
    close();
  }

  /**
   * 删除数据
   *
   * @param tableName 表名
   * @param rowKey    行键
   * @param colFamily 列族名
   * @param col       列名
   * @throws IOException
   */
  public static void deleteRow(String tableName, String rowKey, String colFamily, String col) throws IOException {
    init();
    //获取表对象
    Table table = connection.getTable(TableName.valueOf(tableName));
    //构建删除对象
    Delete delete = new Delete(rowKey.getBytes());
    //删除指定列族的所有数据
    //delete.addFamily(colFamily.getBytes());
    //删除指定列的数据(只删除最新的版本，如果想删除所有版本，使用.addColumns())
    //delete.addColumn(colFamily.getBytes(), col.getBytes());
    //执行删除操作(如果上两行代码被注释，删除指定rowkey的所有数据)
    table.delete(delete);
    table.close();
    close();
  }

  /**
   * 根据行键rowkey查找数据(get)
   *
   * @param tableName 表名
   * @param rowKey    行键
   * @param colFamily 列族名
   * @param col       列名
   * @throws IOException
   */
  public static void getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
    init();
    //声明一个表对象，用于对表信息的管理
    Table table = connection.getTable(TableName.valueOf(tableName));
    //声明一个Get对象，用于查找某个数据
    Get get = new Get(rowKey.getBytes());  //给出行键
    get.addColumn(colFamily.getBytes(), col.getBytes());  //给出列族和列
    //获取的result数据是结果集
    Result result = table.get(get);
    //格式化输出获取的数据
    showCell(result);
    table.close();  //关闭表
    close();
  }

  /**
   * 使用scan方式获取表中数据（全表扫描/startRow~stopRow）
   *
   * @param tableName
   * @throws IOException
   */
  public  static void scanTable(String tableName) throws IOException {
    init();
    //获取表对象
    Table table = connection.getTable(TableName.valueOf(tableName));

    //创建Scan对象(空参构造对应扫描全表)
    //Scan scan = new Scan();0
    //创建Scan对象(扫描范围为startRow~stopRow)(左闭右开)
    Scan scan = new Scan();

    TimeEncoding timeEncoding = new ConcatenationTimeEncoding(new TimePointDimensionDefinition(Calendar.HOUR));

    TimeValue timeValue1 = new TimeValue(2020, 10, 7, 0, 0, 0);
    ByteArray time1 = timeEncoding.getIndex(timeValue1).getStart();
    ByteArray rowKey1 = new ByteArray("57249").combine(time1);

    TimeValue timeValue2 = new TimeValue(2020, 10, 7, 2, 0, 0);
    ByteArray time2 = timeEncoding.getIndex(timeValue2).getStart();
    ByteArray rowKey2 = new ByteArray("57249").combine(time2);

    scan.withStartRow(rowKey1.getBytes(), true);
    scan.withStopRow(rowKey2.getBytes(), true);
    //指定只返回f列族中的tigan列
    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("tigan"));

    //扫描表
    ResultScanner resultScanner = table.getScanner(scan);
    //解析resultScanner
    for (Result result : resultScanner) {
      //解析result并打印
      showCell(result);
      //直接打印result
      //System.out.println(result);
    }
    //关闭表连接
    table.close();
    close();
  }

  /**
   * 格式化输出
   *
   * @param result
   */
  public static void showCell(Result result) {
    Cell[] cells = result.rawCells();
    for (Cell cell : cells) {
      // 也可使用Bytes.toString(),效果与new String()相同
      System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
      System.out.println("Timetamp:" + cell.getTimestamp() + " ");
      System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
      System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
      System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
    }
  }
}
