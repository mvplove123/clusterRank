package cluster.service

import cluster.model.{PoiBound, CellCut}
import cluster.service.impl.{PoiCustomBoundService, PoiPolygonBoundService, StructureBoundService}
import cluster.utils.{WordUtils, GBKFileOutputFormat, Constants, PointUtils}
import com.vividsolutions.jts.geom.{Point, GeometryFactory, Coordinate, Geometry}
import com.vividsolutions.jts.io.WKTReader
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.NumericRange
import scala.collection.mutable
import scala.util.control.Breaks._
import com.esotericsoftware.kryo.Kryo

/**
  * Created by admin on 2016/10/25.
  */
class GpsPopularityService  extends Serializable{

  val cellcut: CellCut = new CellCut(50, 50)


  def gpsPopularity(sc: SparkContext, poiRdd: RDD[String], structureRdd: RDD[String], polygonRdd: RDD[String],
                    gpsRdd: RDD[String]):
  RDD[(String, Int)] = {

    val poiBound = boundCombine(sc, poiRdd, structureRdd, polygonRdd).map(_._2)

    //    poiBound.saveAsTextFile("D:\\structure\\spark\\result1")
    //
    //        val strpath = "/user/go2data_rank/taoyongbo/output/poiBound/"
    //        val path = new Path(strpath)
    //        WordUtils.delDir(sc,path,true)
    //        poiBound.map(x=>(null,x)).saveAsNewAPIHadoopFile(strpath, classOf[Text], classOf[IntWritable],
    //          classOf[GBKFileOutputFormat[Text, IntWritable]])

    val poiGpsPopularity = gpsCombine(sc, poiBound, gpsRdd)
    return poiGpsPopularity
  }

  def gpsPopularity1(sc: SparkContext, boundRdd: RDD[String], gpsRdd: RDD[String]):
  RDD[(String, Int)] = {

    val poiGpsPopularity = gpsCombine(sc, boundRdd, gpsRdd)
    return poiGpsPopularity
  }


  /**
    * 区域合并
    *
    * @param sc
    * @param poiRdd
    * @param structureRdd
    * @param polygonRdd
    * @return
    */
  def boundCombine(sc: SparkContext, poiRdd: RDD[String], structureRdd: RDD[String], polygonRdd: RDD[String]): RDD[(String, String)] = {

    val poiCustomBoundService = new PoiCustomBoundService
    val poiPolygonBoundService = new PoiPolygonBoundService
    val structureBoundService = new StructureBoundService

    val structureBound = structureBoundService.getPoiBound(poiRdd, structureRdd)


    val poiPolygonBound: RDD[(String, String)] = poiPolygonBoundService.getPoiBound(polygonRdd)
    val poiCustomBound: RDD[(String, String)] = poiCustomBoundService.getPoiBound(poiRdd)

    val boundRdd: RDD[(String, String)] = sc.union(poiCustomBound, structureBound, poiPolygonBound)

    val poiBoundListRdd: RDD[(String, List[String])] = boundRdd.combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    )

    val poiBoundRdd = poiBoundListRdd.map(x => filterBound(x._2)).filter(_.isDefined).map(_.get)

    return poiBoundRdd


  }


  /**
    * 区域筛选
    *
    * @param poiBoundList
    * @return
    */
  def filterBound(poiBoundList: List[String]): Option[(String, String)] = {

    var polygonBoundInfo = ""
    var structureBoundInfo = ""
    var customBoundInfo = ""


    poiBoundList.foreach(
      x => {
        if (x.startsWith("polygon_")) polygonBoundInfo = x
        else if (x.startsWith("structure_")) structureBoundInfo = x
        else customBoundInfo = x.substring(7)
      }
    )
    if (StringUtils.isNoneBlank(customBoundInfo)) {
      var boundInfo = ""
      if (StringUtils.isNoneBlank(polygonBoundInfo)) boundInfo = polygonBoundInfo
      else if (StringUtils.isNoneBlank(structureBoundInfo)) boundInfo = structureBoundInfo
      else boundInfo = customBoundInfo


      val boundArray = boundInfo.split("\t")
      if (boundArray.length == 3) {
        val dataId = boundArray(0)
        val name = boundArray(1)
        val point = boundArray(2)
        return Option(dataId, Array(dataId, name, point).mkString("\t"))
      }

    }
    return None
  }

  /**
    * 区域关联gps，返回结果
    *
    * @param sc
    * @param poiRdd
    * @param gpsRdd
    * @return
    */
  def gpsCombine(sc: SparkContext, poiRdd: RDD[String], gpsRdd: RDD[String]): RDD[(String, Int)] = {



    val gpsCount = gpsRdd.map(x => getGpsCell(x, cellcut)).reduceByKey(_ + _).map(x => (x._1, "gpsCount_" + x._2.toString))

    val crossCellflmap = poiRdd.flatMap(x => getCell(x, cellcut)).reduceByKey(_ + "," + _)

    val poiGpsCounts = sc.union(crossCellflmap, gpsCount).repartition(1000)


    val poiGpsCountsCombine = poiGpsCounts.combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    )

    val poiGpsCountsRDD = poiGpsCountsCombine.flatMap(x => sumCellCount(x._2)).reduceByKey(_+_)

    return poiGpsCountsRDD
  }


  /**
    * 获取gps网格数据
    *
    * @param gpsCountStr
    * @param cellcut
    * @return
    */
  def getGpsCell(gpsCountStr: String, cellcut: CellCut): (String, Int) = {
    val gpsCountArray = gpsCountStr.split("\t")
    val poiBound = gpsCountArray(0)
    val gpsCount = gpsCountArray(1).toInt
    val point = poiBound.split(",")
    val currentCell = cellcut.getCurrentCell(point(0), point(1))
    return (currentCell, gpsCount)

  }


  def getCell(poiBoundStr: String, cellcut: CellCut): Array[(String, String)] = {

    val poiBoundArray = poiBoundStr.split('\t')

    var dataId = poiBoundArray(0)
    val bound = poiBoundArray(2)
    //片区数据
    if (dataId.startsWith("polygon_")) {
      dataId = poiBoundArray(0).substring(8)
      val points: Array[String] = bound.split(",")
      return points.map(x => x.split(" ")).map(x => (cellcut.getCurrentCell(x(0), x(1)), dataId))

    } else {
      //poi，结构化数据
      val boundPolygons = bound.split(",")
      var realDataId = ""
      if (dataId.startsWith("structure_")) realDataId = dataId.substring(10) else realDataId = dataId

      val crossCells = cellcut.getCrossCell(boundPolygons(0).toDouble, boundPolygons(1).toDouble, boundPolygons(2)
        .toDouble, boundPolygons(3).toDouble).toArray.map(x => (x, realDataId))
      return crossCells
    }
  }


  def sumCellCount(cellfields: List[String]):Array[(String, Int)] = {
    var gpsSumCount = 0
    var dataIds =Array[String]()
    cellfields.foreach(
      x => if (x.startsWith("gpsCount_")) gpsSumCount = x.substring(9).toInt else dataIds = x.split(",")
    )
    val dataIdsCount: Array[(String, Int)] = dataIds.map(x=>(x,gpsSumCount))
    return dataIdsCount
  }


  def main(args: Array[String]) {

    var bound = "POLYGON ((13507592.6893179 3639152.1053699, 13506673.1981302 3638954.05114788, 13506277.0923554 3638883.32319115, 13506248.7990857 3638628.71051853, 13505796.146847 3638600.41136425, 13505697.1204033 3636818.02301099, 13505470.7742463 3636789.72385671, 13505470.7742463 3636082.42436046, 13505301.0146285 3636039.99555802, 13505329.3078981 3635799.51253356, 13505767.8535774 3635785.36295642, 13505824.4401167 3634710.2740994, 13506758.0378637 3633323.9742612, 13507465.3696045 3633423.00137221, 13507847.2886691 3633804.94031012, 13507635.0891469 3633918.09706929, 13507550.2494134 3633960.54580071, 13507536.1027786 3634229.30805048, 13507889.7285736 3634342.48473863, 13507804.8487647 3635771.21337927, 13508314.1276181 3635771.21337927, 13508384.8607921 3635856.09091314, 13508328.2742529 3636704.84632284, 13508186.8079047 3636690.6967457, 13508073.6348262 3636803.87343385, 13507833.1420343 3636775.57427956, 13507804.8487647 3636973.62850158, 13507748.2622254 3637030.20688117, 13507677.5290513 3637044.35645831, 13507564.3960482 3638218.47242634, 13507635.0891469 3638289.20038306, 13507592.6893179 3639152.1053699))"
    val wktReader = new WKTReader
    val geometry = wktReader.read(bound)
    val boundPolygons: Array[(Double, Double)] = geometry.getCoordinates.map(coordinate => (coordinate.x, coordinate.y))

    val maxX = boundPolygons.maxBy(_._1)._1.toDouble
    val maxY = boundPolygons.maxBy(_._2)._2.toDouble
    val minX = boundPolygons.minBy(_._1)._1.toDouble
    val minY = boundPolygons.minBy(_._2)._2.toDouble


    val xpoint: NumericRange[Double] = Range.Double(minX, maxX, 50.0)
    val ypoint: NumericRange[Double] = Range.Double(minY, maxY, 50.0)
    var xpointList = List[Double]()
    var ypointList = List[Double]()

    var currentPointX = minX
    while (currentPointX < maxX) {
      xpointList ::= currentPointX
      currentPointX += 50.0
    }

    var currentPointY = minY
    while (currentPointY < maxY) {
      ypointList ::= currentPointY
      currentPointY += 50.0
    }


    var list = List[Point]()

    xpoint.foreach(
      x => ypoint.foreach(
        y => {
          list ::= (new GeometryFactory).createPoint(new Coordinate(x, y))
        }
      )
    )

    var list1 = List[String]("a", "b", "c")

    val array = list1.mkString(",")


    list.foreach(
      x => {

        if (x.within(geometry)) {
          println("in,true")
        } else {
          println("out,fause")
        }

      }

    )

    println("ok")


  }

//  override def registerClasses(kryo: Kryo): Unit = {
//    kryo.register(classOf[GpsPopularityService])
//  }
}
