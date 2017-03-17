package cluster.service

import org.apache.spark.rdd.RDD


/**
  * Created by admin on 2016/9/18.
  */
trait PoiBoundService extends Serializable{

  def getPoiBound(args:RDD[String]*):RDD[(String, String)]


  /**
    * get bound
    *
    * @param xList
    * @param yList
    * @return
    */
  def getBoundXY(xList: List[String], yList: List[String]): String = {
    if (xList.isEmpty || xList.isEmpty) {
      return ""
    }
    val xMin: Double =xList.minBy(_.toDouble).toDouble

    val xMax: Double = xList.maxBy(_.toDouble).toDouble
    val yMin: Double = yList.minBy(_.toDouble).toDouble
    val yMax: Double = yList.maxBy(_.toDouble).toDouble
    return Array(xMin,xMax,yMin,yMax).mkString(",")
  }

  def getCrossCellXY (sx: String, sy: String , dim:Int):String={
    val r: Double = dim
    val x: Double = sx.toDouble
    val y: Double = sy.toDouble
    val xl: Double = x - r
    val xh: Double = x + r
    val yl: Double = y - r
    val yh: Double = y + r
    return xl + "," + xh + "," + yl + "," + yh
  }





}
