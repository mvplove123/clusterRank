package cluster.service.impl

import cluster.service.StructureService
import cluster.utils.PointUtils
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2016/10/24.
  */
class StructureInfoService extends StructureService {

  def StructureRDD(poiRdd: RDD[String], structureRdd: RDD[String]): RDD[String] = {

    val poi = poiRdd.map(line => line.split('\t')
    ).map(line => (line(11), Array("poi_" + line(1), line(0), line(3), line(5), line(2)).mkString("\t"))).cache() //dataid,
    // name,
    // city,
    // category,point

    val guidparentChildrenInfo: RDD[(String, String)] = mapChildrenInfo(poi, structureRdd)
    val guidParent: RDD[(String, List[String])] = mapParentInfo(poi, guidparentChildrenInfo)
    val structuresInfoRdd: RDD[String] = guidParent.map(x => structureInfo(x._2))

    return structuresInfoRdd
  }

  def structureInfo(elem: List[String]): String = {

    var parentInfo = ""
    var childsInfo = ""

    elem.foreach(y => {
      if (y.startsWith("poi_")) parentInfo = y.substring(4) else childsInfo = compute(y)
    }
    )
    val structureStr = Array(parentInfo, childsInfo).mkString("\t")

    return structureStr
  }


  def compute(childrenInfo: String): String = {
    var childsResult: String = ""
    try{
      val childArray = childrenInfo.split("\\|")
      val childNum: Int = childArray.size
      var doorNum: Int = 0
      var internalSceneryNum: Int = 0
      var buildNum: Int = 0
      var parkNum: Int = 0
      var area: Int = 0

      var childs = List[String]()

      var xlist = List[String]()
      var ylist = List[String]()

      childArray.foreach(
        child => {
          val childInfo = child.split("\t")
          val name = childInfo.apply(1).trim
          val subCategory = childInfo.apply(3)
          val point = childInfo.apply(4).split(",")
          xlist ::= point(0)
          ylist ::= point(1)

          if (subCategory.equals("停车场")) parkNum += 1
          if (subCategory.equals("景点")) internalSceneryNum += 1
          if (subCategory.equals("楼号") || name.endsWith("楼")) buildNum += 1
          if (subCategory.equals("大门") || name.endsWith("门") && !(subCategory.equals("景点"))) doorNum += 1

          childs ::= name
        }
      )

      val childsStr = childs.toArray.mkString(",")
      val boundFields = PointUtils.getBoundXY(xlist, ylist)

      if (!boundFields.isEmpty) {
        val boundxy: Array[String] = boundFields.split(",")
        val x: Double = boundxy(1).toDouble - boundxy(0).toDouble
        val y: Double = boundxy(3).toDouble - boundxy(2).toDouble
        area = (x * y).toInt
      }
      childsResult = Array(area, childNum, doorNum, parkNum, internalSceneryNum, buildNum, childsStr).mkString("\t")
    }


    catch {
      case ex: Exception => {
        println(childrenInfo)
        None
      }
    }
    return childsResult
  }


}
