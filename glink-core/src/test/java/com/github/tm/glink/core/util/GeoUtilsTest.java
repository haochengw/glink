package com.github.tm.glink.core.util;

import com.github.tm.glink.core.distance.DistanceCalculator;
import com.github.tm.glink.core.distance.GeographicalDistanceCalculator;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.util.Random;

import static org.junit.Assert.*;

public class GeoUtilsTest {

  private GeometryFactory factory = new GeometryFactory();

  @Test
  public void calcDistanceTest() {
    Point p1 = factory.createPoint(new Coordinate(114, 34));
    Point p2 = factory.createPoint(new Coordinate(115, 35));
    double dis = GeoUtils.calcDistance(p1, p2);
    System.out.println(dis);
  }

  @Test
  public void calcBoxByDistTest() {
    Point p = factory.createPoint(new Coordinate(114, 34));
    Envelope envelope = GeoUtils.calcBoxByDist(p, 0.1);
    System.out.println(envelope);
  }

  @Test
  public void generateRandomDataForRangeJoin() {
    Point p1 = factory.createPoint(new Coordinate(114, 34));

  }

  @Test
  public void generateRandomDataForDistanceJoin() {
    Point p1 = factory.createPoint(new Coordinate(114, 34));
    Envelope envelope = GeoUtils.calcBoxByDist(p1, 1);
    System.out.println(envelope);
    double lngStep = envelope.getMaxX() - envelope.getMinX();
    double latStep = envelope.getMaxY() - envelope.getMinY();
    Random random = new Random();
    for (int i = 1; i <= 10; ++i) {
      double lat = envelope.getMinY() + random.nextDouble() * latStep;
      double lng = envelope.getMinX() + random.nextDouble() * lngStep;
      Point point = factory.createPoint(new Coordinate(lng, lat));
      if (GeoUtils.calcDistance(point, p1) > 1) System.out.println(lat + ", " + lng + ", " + false);
      else System.out.println(lat + "," + lng + ", " + true);
    }
  }

  @Test
  public void test() throws ParseException {
    String wkt = "POLYGON ((116.40581333 39.9624509900001,116.4078349 39.960589355,116.409407995 39.9606222900001,116.418825405 39.9607934600001,116.423408295 39.9581081250001,116.423128055 39.952766135,116.422899085 39.9511392650001,116.4228984 39.9504914700001,116.42291317 39.950061965,116.42304127 39.9497560850001,116.4237884325 39.9489129925001,116.428302161667 39.9490212616667,116.42942853 39.9500007550001,116.429091425 39.950860635,116.432806765 39.9508145100001,116.433105205 39.950513025,116.43526763 39.9469450300001,116.440336875 39.944757145,116.440595835 39.94456488,116.441052785 39.9438592700001,116.4411199975 39.9436616037501,116.440402205 39.9417096600001,116.44028122 39.9414403050001,116.440194505 39.9412847300002,116.440043815 39.941141735,116.43977278 39.9409905500001,116.438693245 39.9404446550001,116.43852562 39.9403306225001,116.438450365 39.94022173,116.438430705 39.940117,116.43767079 39.935937645,116.4376327125 39.9356923600001,116.43762138 39.9352612550001,116.437606435 39.9331056,116.437590075 39.930009505,116.43756321 39.9273115100001,116.431040645 39.9269891100001,116.43091834 39.9270279750001,116.430774665 39.9272386250001,116.43072214 39.9280065900001,116.430671435 39.9282083150001,116.4281103175 39.9281711400001,116.42820049 39.9253787,116.428298845 39.9231413800001,116.428355265 39.921857925,116.42849924 39.91815786,116.42855072 39.9168864700001,116.42862335 39.9156150750001,116.428681395 39.9143149350001,116.428736875 39.9130722900001,116.42879981 39.9125832600001,116.42896272 39.9118052550001,116.42911557 39.9110753450001,116.42922091 39.9106433400001,116.429378795 39.9099016100001,116.4295147075 39.9070830425002,116.42949179 39.90681059,116.429873035 39.903647885,116.43019794 39.9011154450001,116.43115146 39.9011213716667,116.435825325 39.9017169450001,116.442335535 39.9020354,116.4423491275 39.9018827650001,116.44257935 39.9011295450001,116.44267409 39.900784925,116.44270272 39.9006785900001,116.44276749 39.9004379450002,116.442778825 39.9001283,116.44278847 39.8998646450001,116.442789445 39.8997425350001,116.44263791 39.8997176800001,116.43988731 39.898701765,116.44097582 39.8927133850001,116.441443645 39.8922933350001,116.4447066 39.8922527350001,116.44491647 39.8907092900002,116.44468643 39.8888652600001,116.44433939 39.8888764550001,116.44202986 39.88942767,116.439780585 39.8898454050001,116.4378467225 39.8886818675001,116.43948307 39.8775178525001,116.43717505 39.8714298850001,116.43695829 39.870863905,116.436853341 39.8705152800001,116.41683345 39.8714383250001,116.41485011 39.8713979250001,116.41485398 39.8712852350001,116.410622085 39.8693046500001,116.408880535 39.8674690300001,116.40951168 39.86539729,116.409544675 39.86458717,116.409518145 39.8640888350001,116.40885453 39.86222037,116.40731274 39.8586611500001,116.40490681 39.8575703,116.40423467 39.8575303550001,116.403635725 39.8575145,116.403166885 39.857512085,116.401840395 39.8575618200002,116.40064761 39.8585776250001,116.3943847 39.8582640750001,116.38918876 39.8583353650001,116.389136935 39.8599063750001,116.3887662 39.8613899300001,116.38493146 39.8638716450002,116.38155665 39.8646840500001,116.378638685 39.8649859900001,116.374964385 39.86661705,116.37517284 39.8693873850001,116.375184733 39.8695670140001,116.392984845 39.8707002050001,116.393172765 39.8707022200001,116.39316447 39.8711611850001,116.392821885 39.8795729350001,116.392776755 39.8808871300002,116.39267276 39.8822721650001,116.3923669 39.8851017100001,116.392370515 39.8863476500001,116.392369515 39.886606655,116.39223275 39.8879656700001,116.39215272 39.8892103200001,116.3920847325 39.8899028325001,116.3921903475 39.8899797450001,116.3917855 39.8962318550001,116.3917725 39.8964288550001,116.39167958 39.89742407,116.389754725 39.9016022700001,116.386523255 39.9071775300001,116.3855638 39.9092929350001,116.385462905 39.9104899700001,116.386070195 39.9209243550001,116.39056868 39.9210516250001,116.393399645 39.9220400750001,116.39334606 39.9239187725001,116.39322544 39.926470725,116.39319751 39.9270127500001,116.390526255 39.9270161500001,116.39028946 39.9270164450001,116.389959305 39.9339654350001,116.38978217 39.9380381300001,116.3897317 39.9389209350001,116.388903635 39.9392645150001,116.387972715 39.9393143200001,116.387824435 39.9393288350001,116.38751554 39.9475964250001,116.38753868 39.9493468800001,116.38746037 39.9522291700001,116.38730784 39.955723555,116.38633249 39.9556813900001,116.38360765 39.95552682,116.380381165 39.9553432800001,116.3805338 39.9561416800001,116.38138864 39.960056665,116.38330242 39.9616532550001,116.39106194 39.9618821650002,116.39164589 39.9613300950001,116.39454123 39.960625815,116.39506961 39.9605750600001,116.39875364 39.9604026300001,116.401633860833 39.9606844300001,116.40156486 39.9634525600001,116.40152687 39.9637645650001,116.40151065 39.96493421,116.40116685 39.9667456300001,116.401122845 39.96751169,116.40112433 39.969025715,116.40128299 39.969662285,116.40132035 39.9708138400001,116.40127105 39.9726145300001,116.40148861 39.9726180550001,116.40311044 39.9726635,116.40495703 39.9699564450001,116.4050132 39.9678944650001,116.405007645 39.966794765,116.40501711 39.9636616550001,116.40581333 39.9624509900001))";
    WKTReader wktReader = new WKTReader();
    Polygon polygon = (Polygon) wktReader.read(wkt);
    System.out.println(polygon.getCoordinates().length);
    Point point = factory.createPoint(new Coordinate(116.417492,39.929765));
    long start = System.currentTimeMillis();
    for (int i = 0; i < 10000; ++i) {
      polygon.contains(point);
    }
    long end = System.currentTimeMillis();
    System.out.println(end - start);

    DistanceCalculator distanceCalculator = new GeographicalDistanceCalculator();
    Point center = factory.createPoint(new Coordinate(113.23456, 32.43587));
    start = System.currentTimeMillis();
    for (int i = 0; i < 1000000; ++i) {
      distanceCalculator.calcDistance(center, point);
    }
    end = System.currentTimeMillis();
    System.out.println(end - start);
  }

}