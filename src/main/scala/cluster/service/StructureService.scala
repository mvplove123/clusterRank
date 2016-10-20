package cluster.service

import cluster.utils.PointUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.xml.sax.SAXParseException

/**
  * Created by admin on 2016/9/14.
  */
object StructureService {

  def StructureRDD(sc: SparkContext, poiRdd: RDD[String], structureRdd: RDD[String]): RDD[String] = {

    val poi = poiRdd.map(line => line.split('\t')
    ).map(line => (line(11), Array("poi_" + line(1), line(0), line(3), line(5), line(2)).mkString("\t")))//dataid,
    // name,
    // city,
    // category,point

    val guidparentChildrenInfo: RDD[(String, String)] = mapChildrenInfo(poi, structureRdd)

    val structuresInfo: RDD[String] = mapParentInfo(poi, guidparentChildrenInfo)

    return structuresInfo
  }


  /**
    * mapChindInfo
    *
    * @param poi
    * @param structureLine
    */
  def mapChildrenInfo(poi: RDD[(String, String)], structureLine: RDD[String]): RDD[(String, String)] = {

    //child structure info
    val childStructure: RDD[(String, String)] = structureLine.map(line => line.split('\t')).map(line => (line(2),
      "guid_" + line(1))).cache()

    //filter poilist with children info
    val guidChilds: RDD[(String, List[String])] = childStructure.union(poi).combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    ).filter(x => x._2.size == 2)

    //(k,v)=>(guid->poi1|poi2|poi3)
    val guidChildStructureList = guidChilds.map(x => getList(x._2)).reduceByKey((x, y) => x + "|" + y)

    return guidChildStructureList

  }

  def getList(elem: List[String]): (String, String) = {
    var key = ""
    var value = ""
    elem.foreach(
      y => if (y.startsWith("guid_")) key = y.substring(5) else value = y.substring(4)
    )
    return (key, value)
  }

  def mapParentInfo(poi: RDD[(String, String)], guidparentChildrenInfo: RDD[(String, String)]): RDD[String] = {

    //filter poilist with parent info
    val guidParent: RDD[(String, List[String])] = guidparentChildrenInfo.union(poi).combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    ).filter(x => x._2.size == 2)

    val result: RDD[String] = guidParent.map(x => structureInfo(x._2))
    return result
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
    try {
      var childArray = childrenInfo.split("\\|")


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

    } catch {
      case ex: Exception => {
        println(childrenInfo)
        None
      }
    }
    return childsResult


  }


}
