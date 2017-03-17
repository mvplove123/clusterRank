package cluster.task

import cluster.model.{CellCut, Poi}
import cluster.service.{GpsPopularityService, PoiService}
import cluster.utils._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession

/**
  * Created by admin on 2016/12/19.
  */
object PolygonRankTask {


  val distance = 10

  val cellcut: CellCut = new CellCut(distance, 50)

  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)
    val polygonRankOutPut = "/user/go2data_rank/taoyongbo/output/polygonRank/"

    val path = new Path(polygonRankOutPut)
    WordUtils.delDir(sc, path, true)

    val polygonRdd: RDD[String] = WordUtils.convert(sc, Constants.polygonXmlPath, Constants.gbkEncoding)
    val gpsRdd = WordUtils.convert(sc, Constants.gpsCountInputPath, Constants.gbkEncoding)
    val  gpsPopularityService = new GpsPopularityService

    //cellId ,dataIds
    val gpsCount: RDD[(String, String)] = gpsRdd.map(x => gpsPopularityService.getGpsCell(x, cellcut)).reduceByKey(_
      + _,1000).map(x => (x._1,
      "gpsCount_" + x._2.toString))



    //解析poi
    val poiRdd: RDD[Poi] = PoiService.getPoiRdd(polygonRdd).repartition(20).cache()

    //poi 字符串化
    val poistr = poiRdd.map(x=>( x.dataId, Array(x.dataId, x.name,x.city,x.category,x.subCategory, x.geometry.replace(" ",","))
      .mkString("\t")))



    val crossCellflmap: RDD[(String, String)] = getPoiBound(poiRdd)

    //片区及热度数据归并
    val poiGpsCounts: RDD[(String, String)] = sc.union(crossCellflmap, gpsCount)

    //数据映射
    val poiGpsCountsCombine = poiGpsCounts.combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2,1000
    )

    val poiGpsCountsRDD: RDD[(String, Int)] = poiGpsCountsCombine.flatMap(x => sumCellCount(x._2)).reduceByKey(_+_,
      1000)


    val result = poistr.join(poiGpsCountsRDD).map(x=>x._2._1+"\t"+x._2._2)

    //计算rank
    val featureValueRdd: RDD[(String, String)] = result.map(x => x.split("\t")).map(x => (WordUtils.converterToSpell(x(2)), x.mkString("\t")))

    val featureSplit: RDD[(String, List[(String, Array[Double])])] = featureValueRdd.combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2,50
    ).mapValues(x => x.map(x => (x.split("\t"))).map(x => (x.mkString("\t"), x.slice(6, 7).map(_.toDouble).map(x =>
      WordUtils
      .covertNum(x))))).cache()


    val rankRdd = MultiThreadCityRank.cityRank(sc,featureSplit)

    rankRdd.saveAsHadoopFile(polygonRankOutPut,
      classOf[Text],
      classOf[IntWritable],
      classOf[RDDMultipleTextOutputFormat])
    sc.stop()

  }


  def sumCellCount(cellfields: List[String]):Array[(String, Int)] = {
    var gpsSumCount = 0
    var dataIds =Array[String]()
    cellfields.foreach(
      x => if (x.startsWith("gpsCount_")) gpsSumCount = x.substring(9).toInt else dataIds = x.split(",")
    )
    val dataIdsCount: Array[(String, Int)] = dataIds.distinct.map(x=>(x,gpsSumCount))
    return dataIdsCount
  }



  def getPoiBound(poiRdd: RDD[Poi]): RDD[(String, String)] = {

    val poiPolygon: RDD[(String, String)] = poiRdd.flatMap(x => getPolygon(x)).reduceByKey(_ + "," + _,1000)
    return poiPolygon
  }

  /**
    * 获取片区
    * @param poi
    * @return
    */
  def getPolygon(poi: Poi): List[(String, String)] = {

    val realDataId = poi.dataId
    val name = poi.name
    val category = poi.category
    val bound = poi.geometry
    val city = poi.city
    val wktReader = new WKTReader

    //筛选有效片区坐标点
    val geometry: Geometry = wktReader.read(bound)
    val boundPolygons: Array[(Double, Double)] = geometry.getCoordinates.map(coordinate => (coordinate.x, coordinate.y))
    val boundPoints: List[String] = PointUtils.getBoundPoint(boundPolygons, distance).filter(_.within(geometry)).map(x => x.getX
      .toString + " " + x.getY.toString)

    val realBound = boundPoints.mkString(",")


    val result: List[(String, String)] = boundPoints.map(x => x.split(" ")).map(x => (cellcut.getCurrentCell(x(0), x(1)), realDataId))

    return result

  }


}
