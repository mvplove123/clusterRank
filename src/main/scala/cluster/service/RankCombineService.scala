package cluster.service

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2016/10/18.
  */
object RankCombineService {


  def rankCombineRDD(sc: SparkContext, multiRank: RDD[String], hotCountRank: RDD[String]): RDD[String] = {


    val multiMap = multiRank.map(x => x.split('\t')).map(x => (x(1), "multi_" + x.mkString("\t")))

    val hotCountMap = hotCountRank.map(x => x.split('\t')).map(x => (x(0), "hotCount_" + Array(x(1),x(2)).mkString
    ("\t")))


    val combineRank = sc.union(multiMap, hotCountMap)

    val result = combineRank.combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    ).filter(x => x._2.length == 2).map(x => combineLine(x._2))

    return result
  }

  def combineLine(elem: List[String]): String = {
    var multi = ""
    var hotCount = ""
    elem.foreach(
      y => {
        if (y.startsWith("multi_")) multi = y.substring(6)
        if (y.startsWith("hotCount_")) hotCount = y.substring(9)
      }
    )
    //null代表无聚类结果
    return Array(multi, hotCount," "," ").mkString("\t")
  }


}
