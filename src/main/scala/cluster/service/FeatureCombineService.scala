package cluster.service

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2016/10/10.
  * 原始特征值合并
  */
object FeatureCombineService {


  def CombineRDD(sc: SparkContext, matchCountRdd: RDD[String], searchCountRdd: RDD[String], poiHotCountRdd: RDD[String],
                 structureRdd: RDD[String], poiRdd: RDD[String]): RDD[String] = {


    val matchCount = matchCountRdd.map(line => line.split('\t')
    ).map(line => (line(1), "matchCount_" + line(6))) //dataid,matchCount

    val searchCount = searchCountRdd.map(line => line.split('\t')
    ).map(line => (line(0), Array("searchCount_" + line(5), line(6), line(7)).mkString("\t"))) //dataid,hitCount,viewCount,sumViewOrder

    val hotCount = poiHotCountRdd.map(line => line.split('\t')
    ).map(line => (line(0),"hotCount_" + line(2))) //dataid,hotCount

    val structureInfo = structureRdd.map(line => line.split('\t')
    ).filter(x=>x.length==12).map(line => (line(0), Array("structure_" + line(5), line(6), line(7), line(8), line(9),
      line(10))
      .mkString
    ("\t"))) //dataid,
    // area,childNum,doorNum,parkNum,internalSceneryNum, buildNum

    val poi = poiRdd.map(line => line.split('\t')
    ).map(line => (line(1), Array("poi_" + line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8),
      line(9), line(10)).mkString("\t")))
    //name,dataId,point,city,category,subCategory,brand, realKeyword,commentNum, price, grade


    val combineFeatures: RDD[(String, List[String])] = sc.union(poi, matchCount, structureInfo, searchCount,
      hotCount).combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    )

    val combineRdd: RDD[String] = combineFeatures.map(x => featureCombineInfo(x._2)).filter(x=>StringUtils.isNotBlank
    (x)).cache()

    return combineRdd

  }


  def featureCombineInfo(elem: List[String]): String = {
    //引用次数
    var matchCountValue: String = "0"
    //结构化数据
    var structureValue: String = Array("0", "0", "0", "0", "0", "0").mkString("\t")
    //poi数据
    var poiValue: String = ""
    //热点人气数据
    var hotCountValue: String = "0"
    //点击量，搜索量
    var searchCountValue: String = Array("0", "0", "0").mkString("\t")

    elem.foreach(y => {
      if (y.startsWith("matchCount_")) matchCountValue = y.substring(11)
      else if (y.startsWith("searchCount_")) searchCountValue = y.substring(12)
      else if (y.startsWith("hotCount_")) hotCountValue = y.substring(9)
      else if (y.startsWith("structure_")) structureValue = y.substring(10)
      else if (y.startsWith("poi_")) poiValue = y.substring(4)
    }
    )

    if (StringUtils.isBlank(poiValue)) return ""
    else
      return Array(poiValue, matchCountValue, structureValue, hotCountValue, searchCountValue).mkString("\t")

  }


}
