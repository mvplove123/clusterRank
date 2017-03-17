package cluster.service

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2016/10/18.
  */
object RankCombineService {


  def rankCombineRDD(sc: SparkContext, multiRank: RDD[String], hotCountRank: RDD[String],hitCountRank: RDD[String]):
  RDD[String] = {

    try{
      val multiMap = multiRank.map(x => x.split('\t')).map(x => (x(1), "multi_" + x.mkString("\t")))

      val hotCountMap = hotCountRank.map(x => x.split('\t')).map(x => (x(0), "hotCount_" + Array(x(1),x(2)).mkString
      ("\t")))

      val hitCountMap = hitCountRank.map(x => x.split('\t')).map(x => (x(0), "hitCount_" + Array(x(1),x(2)).mkString
      ("\t")))

      val combineRank = sc.union(multiMap, hotCountMap,hitCountMap)

      val result = combineRank.combineByKey(
        (v: String) => List(v),
        (c: List[String], v: String) => v :: c,
        (c1: List[String], c2: List[String]) => c1 ++ c2
      ).filter(x => x._2.length == 3).map(x => combineLine(x._2))

      return result
    } catch {
      case ex: Exception => {
        return null
      }
    }

  }

  def combineLine(elem: List[String]): String = {
    var multi = ""
    var hotCount = ""
    var hitCount = ""
    elem.foreach(
      y => {
        if (y.startsWith("multi_")) multi = y.substring(6)
        if (y.startsWith("hotCount_")) hotCount = y.substring(9)
        if (y.startsWith("hitCount_")) hitCount = y.substring(9)
      }
    )
    //null代表无聚类结果
    return Array(multi, hotCount,hitCount).mkString("\t")
  }


}
