package cluster.service.impl

import cluster.model.Poi
import cluster.service.{PoiBoundService, PoiService}
import cluster.utils.PointUtils
import com.vividsolutions.jts.geom.{Point, Geometry}
import com.vividsolutions.jts.io.WKTReader
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2016/9/18.
  */
class PoiPolygonBoundService extends PoiBoundService{

  val filterCategory = List[String]("地名")


  def getPoiBound(args: RDD[String]*): RDD[(String, String)] ={

    val poiLine: RDD[String] = args.apply(0)
    val poiRdd: RDD[Poi] = PoiService.getPoiRdd(poiLine)
    val poiPolygon = poiRdd.map(x => getPolygon(x)).filter(_!=null).combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    ).filter(_._2.size==1).map(x=>(x._1,x._2.apply(0)))

    return poiPolygon
  }

  def getPolygon(poi: Poi): (String, String) = {


    //片区主点
    val keyword = poi.keyword

    val category = poi.category
    if(StringUtils.isNotBlank(keyword) && !filterCategory.contains(category)){
      val regex="""\$\$POI:(.*)-(.*)\$\$""".r
      val regex(dataId, name) = keyword
      val bound = poi.geometry
      val realDataId="1_"+dataId
      val wktReader = new WKTReader

      val geometry: Geometry = wktReader.read(bound)
      val boundPolygons: Array[(Double, Double)] = geometry.getCoordinates.map(coordinate => (coordinate.x, coordinate.y))
      val boundPoints: List[String] = PointUtils.getBoundPoint(boundPolygons, 50.0).filter(_.within(geometry)).map(x=>x.getX
        .toString+" "+x.getY.toString)

      val realBound = boundPoints.mkString(",")

      val value = Array(realDataId,name,realBound).mkString("\t")
      return (realDataId,"polygon_"+value)
    }
    return null
  }




}
