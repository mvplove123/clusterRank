package cluster.service.impl

import cluster.service.{PoiBoundService, StructureService}
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2016/9/18.
  */
class StructureBoundService extends StructureService with PoiBoundService {

  def getPoiBound(args: RDD[String]*): RDD[(String, String)] = {

    val poiRdd = args.apply(0)
    val structureRdd = args.apply(1)

    val structuresInfo = StructureRDD(poiRdd,structureRdd)

    val structureBound: RDD[(String, String)] = structuresInfo.map(x=>(x.split("\t")(0)->("structure_"+x)))

    return structureBound

  }


  /**
    * 结构化rdd
    *
    * @param poiRdd
    * @param structureRdd
    * @return
    */
  def StructureRDD(poiRdd: RDD[String], structureRdd: RDD[String]): RDD[String] = {

    val poi = poiRdd.map(line => line.split('\t')
    ).map(line => (line(11), Array("poi_" + line(1), line(0),line(2)).mkString("\t"))) //dataid,
    // name,
    //point

    val guidparentChildrenInfo: RDD[(String, String)] = mapChildrenInfo(poi, structureRdd)
    val guidParent: RDD[(String, List[String])] = mapParentInfo(poi, guidparentChildrenInfo)
    val structuresInfo: RDD[String] = guidParent.map(x => structureInfo(x._2))
    return structuresInfo

  }

  /**
    * 返回结构化信息
    *
    * @param elem
    * @return
    */
  def structureInfo(elem: List[String]): String = {

    var parentInfo = ""
    var childsBound = ""

    elem.foreach(y => {
      if (y.startsWith("poi_")) parentInfo = y.substring(4) else childsBound = childsArea(y)
    }
    )

    val parentInfoArray=parentInfo.split("\t")
    val dataId = parentInfoArray(0)
    val name = parentInfoArray(1)

    val structureStr = Array(dataId,name, childsBound).mkString("\t")
    return structureStr
  }

  /**
    * 获取片区边界信息
    *
    * @param childrenInfo
    * @return
    */
  def childsArea(childrenInfo: String): String = {

    val childArray: Array[String] = childrenInfo.split("\\|")
    var xList = List[String]()
    var yList = List[String]()

    childArray.foreach(
      child => {
        val childInfo: Array[String] = child.split("\t")
        if(childInfo.length==3){
          val point = childInfo(2).split(",")
          xList ::= point(0)
          yList ::= point(1)
        }

      }

    )
    val boundFields = getBoundXY(xList, yList)
    return boundFields
  }


}
