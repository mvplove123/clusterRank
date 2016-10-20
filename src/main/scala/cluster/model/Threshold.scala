package cluster.model

/**
  * Created by admin on 2016/10/14.
  */
class Threshold extends Serializable{
  /**
    * 范围区间
    */
  var categoryThreshold: Array[Int] = null
  /**
    * 范围取值
    */
  var thresholdValue: Array[Int] = null
  var rankThreshold: Array[Double] = null
  var rankThresholdValue: Array[Int] = null

  def this(categoryThreshold: Array[Int], thresholdValue: Array[Int]) {
    this()
    this.categoryThreshold = categoryThreshold
    this.thresholdValue = thresholdValue
  }

  def this(rankThreshold: Array[Double], rankThresholdValue: Array[Int]) {
    this()
    this.rankThreshold = rankThreshold
    this.rankThresholdValue = rankThresholdValue
  }


}
