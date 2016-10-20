package cluster.model
import scala.collection.mutable


/**
  * Created by admin on 2016/10/14.
  */
class FeatureValue extends Serializable{
  /**
    * 分类值
    */
  var categoryValue: String = null
  /**
    * 标签值
    */
  var tagValue: String = null
  /**
    * 区间范围值
    */
  var scopeValue: mutable.Map[String, Threshold] = null




}
