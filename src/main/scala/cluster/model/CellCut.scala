package cluster.model

/**
  * Created by admin on 2016/9/15.
  */
class CellCut extends java.io.Serializable{
  private[model] var DIAMETER: Double = .0
  private[model] var EXPAND: Double = .0

  def this(DIAMETER: Double, EXPAND: Double) {
    this()
    this.DIAMETER = DIAMETER
    this.EXPAND = EXPAND
  }

  def getCurrentCell(sx: String, sy: String): String = {
    var x: Double = sx.toDouble
    var y: Double = sy.toDouble
    x = x / DIAMETER
    val ax: Int = x.toInt
    y = y / DIAMETER
    val ay: Int = y.toInt
    return ax + "|" + ay
  }

  def getCrossCell(xl: Double, xh: Double, yl: Double, yh: Double): List[String] = {

    val xld = xl / DIAMETER
    val axl: Int = xld.toInt
    val yld = yl / DIAMETER
    val ayl: Int = yld.toInt
    val xhd = xh / DIAMETER
    val axh: Int = xhd.toInt
    val yhd = yh / DIAMETER
    val ayh: Int = yhd.toInt
    var outList = List[String]()


    for (i<- axl to axh){
      for (j<- ayl to ayh){
        val temp: String = i + "|" + j
        if (!outList.contains(temp)) outList ::= temp
      }
    }

    return outList
  }

  /**
    * 计算与当前格子相交N米格子的编号
    *
    * @param sx
    * @param sy
    * @return
    */
  def getCrossCell(sx: String, sy: String): List[String] = {
    val r: Double = EXPAND
    val x: Double = sx.toDouble
    val y: Double = sy.toDouble
    val xl: Double = x - r
    val xh: Double = x + r
    val yl: Double = y - r
    val yh: Double = y + r
    return getCrossCell(xl, xh, yl, yh)
  }

  def getCrossCellXY(sx: String, sy: String): String = {
    val r: Double = EXPAND
    val x: Double = sx.toDouble
    val y: Double = sy.toDouble
    val xl: Double = x - r
    val xh: Double = x + r
    val yl: Double = y - r
    val yh: Double = y + r
    return Array(xl,xh,yl,yh).mkString(",")
  }

}
