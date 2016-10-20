package cluster.utils

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
  def getBoundXY(xlist: List[String], ylist: List[String]): String = {

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

  def compareDouble(e1: String, e2: String) = (e1.toDouble < e2.toDouble)


}
