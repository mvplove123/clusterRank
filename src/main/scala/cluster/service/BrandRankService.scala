package cluster.service

import java.text.DecimalFormat

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2016/10/19.
  */
object BrandRankService {


  def brandRankRDD(multiRank: RDD[String]): RDD[String] = {


    val brandMap: RDD[(String, List[String])] = multiRank.map(x => x.split('\t'))
      .filter(x => StringUtils.isNotBlank(x(5)))
      .map(x => (x(5), x.mkString("\t")))
      .combineByKey(
        (v: String) => List(v),
        (c: List[String], v: String) => v :: c,
        (c1: List[String], c2: List[String]) => c1 ++ c2
      )

    val brandRankRdd = brandMap.map(x => computeRank(x)).filter(x=>StringUtils.isNoneBlank(x))
      .sortBy(x=>(x.split('\t')(1).toDouble,x.split('\t')(2).toInt), false)
    return brandRankRdd

  }


  def computeRank(brandMap: (String, List[String])): String = {

    var count: Int = 0
    var sum: Double = 0

    val brandName = brandMap._1
    brandMap._2.foreach(
      x => {
        val fields = x.split('\t')
        val poiRank = fields(36)
        sum += poiRank.toDouble
        count += 1
      }
    )

    val df: DecimalFormat = new DecimalFormat("#.00")

    if (count > 0) {
      val brandRank: String = df.format(sum / count)
      val result: String = Array(brandName, brandRank, count).mkString("\t")
      return result
    }

    return null


  }


}
