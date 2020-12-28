package com.github.tm.glink.sql.udf.standard.output;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;

@SuppressWarnings("checkstyle:TypeName")
public class ST_AsText extends ScalarFunction {

  public transient WKTWriter wktWriter;

  @Override
  public void open(FunctionContext context) throws Exception {
    wktWriter = new WKTWriter();
  }

  public String eval(Geometry geom) {
    return wktWriter.write(geom);
  }
}
