package cluster.service

import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2016/9/14.
  */
trait StructureService extends Serializable{

  def StructureRDD(poiRdd: RDD[String], structureRdd: RDD[String]): RDD[String]


  /**
    * mapChindInfo
    *
    * @param poi
    * @param structureLine
    */
  def mapChildrenInfo(poi: RDD[(String, String)], structureLine: RDD[String]): RDD[(String, String)] = {

    //child structure info
    val childStructure: RDD[(String, String)] = structureLine.map(line => line.split('\t')).map(line => (line(2),
      "guid_" + line(1)))

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

  def mapParentInfo(poi: RDD[(String, String)], guidparentChildrenInfo: RDD[(String, String)]): RDD[(String, List[String])]  = {

    //filter poilist with parent info
    val guidParent: RDD[(String, List[String])] = guidparentChildrenInfo.union(poi).combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    ).filter(x => x._2.size == 2)

    return guidParent
  }




}
