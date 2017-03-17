package cluster.utils

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point}

import scala.collection.immutable.NumericRange

/**
  * Created by admin on 2016/9/15.
  */
object PointUtils {


  /**
    *
    * @param xlist
    * @param ylist
    * @return
    */
  def getBoundXY1(xlist: List[String], ylist: List[String]): String = {

    if (xlist.isEmpty || ylist.isEmpty) {
      return ""
    }

    val sortXlist = xlist.sortWith(compareDouble(_, _))
    val sortYlist = ylist.sortWith(compareDouble(_, _))


    val xmin: Double = sortXlist.apply(0).toDouble
    val xmax: Double = sortXlist.apply(sortXlist.size - 1).toDouble

    val ymin: Double = sortYlist.apply(0).toDouble
    val ymax: Double = sortYlist.apply(sortYlist.size - 1).toDouble

    return Array(xmin, xmax, ymin, ymax).mkString(",")

  }


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
    val xMin: Double =xList.maxBy(_.toDouble).toDouble

    val xMax: Double = xList.maxBy(_.toDouble).toDouble
    val yMin: Double = yList.maxBy(_.toDouble).toDouble
    val yMax: Double = yList.maxBy(_.toDouble).toDouble
    return Array(xMin,xMax,yMin,yMax).mkString(",")
  }


  sealed case class Loc(lat: Double, lng: Double)

  def isPointInPolygon(poly: List[Loc], x: Loc): Boolean = {
    (poly.last :: poly).sliding(2).foldLeft(false) { case (c, List(i, j)) =>
      val cond = {
        (
          (i.lat <= x.lat && x.lat < j.lat) ||
            (j.lat <= x.lat && x.lat < i.lat)
          ) &&
          (x.lng < (j.lng - i.lng) * (x.lat - i.lat) / (j.lat - i.lat) + i.lng)
      }

      if (cond) !c else c
    }
  }

  def compareDouble(e1: String, e2: String) = (e1.toDouble < e2.toDouble)


  /**
    * 获取片区内的间隔步长N的所有坐标点
    *
    * @param boundPolygons
    * @return
    */
  def getBoundPoint(boundPolygons: Array[(Double, Double)],step:Double): List[Point] ={

    val maxX = boundPolygons.maxBy(_._1)._1.toDouble
    val maxY = boundPolygons.maxBy(_._2)._2.toDouble
    val minX = boundPolygons.minBy(_._1)._1.toDouble
    val minY = boundPolygons.minBy(_._2)._2.toDouble

    val xpointList: NumericRange[Double] = Range.Double(minX,maxX,step)
    val ypointList: NumericRange[Double] = Range.Double(minY,maxY,step)

//    var xpointList = List[Double]()
//    var ypointList = List[Double]()
//
//    var currentPointX=minX
//    while(currentPointX<maxX){
//      xpointList::=currentPointX
//      currentPointX+=50.0
//    }
//
//    var currentPointY=minY
//    while(currentPointY<maxY){
//      ypointList::=currentPointY
//      currentPointY+=50.0
//    }


    var pointsList = List[Point]()

    xpointList.foreach(
      x=>ypointList.foreach(
        y=>{
          pointsList::=(new GeometryFactory).createPoint(new Coordinate(x,y))
        }
      )
    )

    return pointsList
  }





}
