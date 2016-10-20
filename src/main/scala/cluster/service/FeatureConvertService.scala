package cluster.service

import cluster.model.{FeatureValue, Threshold}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import util.control.Breaks._
import scala.collection.mutable

/**
  * Created by admin on 2016/10/14.
  */
object FeatureConvertService {


  def FeatureValueRDD(sc: SparkContext, featureCombineRdd: RDD[String], featureThreshold: RDD[String]): RDD[String] = {
    val featureMap = mutable.Map[String, FeatureValue]()


    //    featureThreshold.foreach(
    //
    //      x => featureConvert(x, featureMap)
    //
    //    )

    featureThreshold.collect().map(x => featureConvert(x, featureMap))

    val featureValueRdd: RDD[String] = featureCombineRdd.map(x => getFeatureValue(x.split("\t"), featureMap))
    return featureValueRdd
  }

  /**
    * 转换设值
    *
    * @param featureMap
    * @param record
    */
  def featureConvert(record: String, featureMap: mutable.Map[String, FeatureValue]): Unit = {

    try {


      val featureRank: FeatureValue = new FeatureValue
      val scopeValue = mutable.Map[String, Threshold]()
      val fields: Array[String] = record.split("\t")
      val parentCategory: String = fields(0)
      val subCategory: String = fields(1)
      val category: String = fields(2)
      val tag: String = fields(3)
      val matchCount: String = fields(4)
      val grade: String = fields(5)
      val comment: String = fields(6)
      val price: String = fields(7)
      val area: String = fields(8)
      val leafCount: String = fields(9)
      val doorCount: String = fields(10)
      val parkCount: String = fields(11)
      val innerCount: String = fields(12)
      val buildCount: String = fields(13)
      val uniqueKey: String = parentCategory + "-" + subCategory
      getScopeValue(scopeValue, "matchCount", matchCount)
      getScopeValue(scopeValue, "grade", grade)
      getScopeValue(scopeValue, "comment", comment)
      getScopeValue(scopeValue, "price", price)
      getScopeValue(scopeValue, "area", area)
      getScopeValue(scopeValue, "leafCount", leafCount)
      getScopeValue(scopeValue, "doorCount", doorCount)
      getScopeValue(scopeValue, "parkCount", parkCount)
      getScopeValue(scopeValue, "innerCount", innerCount)
      getScopeValue(scopeValue, "buildCount", buildCount)
      featureRank.categoryValue = category
      featureRank.tagValue = tag
      featureRank.scopeValue = scopeValue
      featureMap += (uniqueKey -> featureRank)
    } catch {
      case ex: Exception => {
        println(ex)
        None
      }
    }

  }


  /**
    * 范围特征值初始化
    *
    * @param scopeValue
    * @param fieldName
    * @param fieldValue
    */
  def getScopeValue(scopeValue: mutable.Map[String, Threshold], fieldName: String, fieldValue: String): Unit = {
    if (StringUtils.isNotBlank(fieldValue)) {
      val kv: Array[String] = fieldValue.split("\\|")
      val fieldThreshold: Array[Int] = strToIntegers(kv(0))
      val thresholdValue: Array[Int] = strToIntegers(kv(1))
      val rank: Threshold = new Threshold(fieldThreshold, thresholdValue)

      scopeValue += (fieldName -> rank)
    }
    else {
      scopeValue += (fieldName -> null)
    }
  }

  /**
    * str 数组转Int数组
    *
    * @param str
    * @return
    */
  def strToIntegers(str: String): Array[Int] = {
    if (StringUtils.isBlank(str)) {
      return null
    }
    val values: Array[String] = str.split(",")
    val newValues = new Array[Int](values.length)
    for (i <- 0 until values.length) {
      if (StringUtils.isNotBlank(values(i))) {
        newValues(i) = values(i).toInt

      }
    }
    return newValues
  }

  /**
    * 解析转换特征值
    *
    * @param line
    * @param featureMap
    * @return
    */
  private def getFeatureValue(line: Array[String], featureMap: mutable.Map[String, FeatureValue]): String = {
    val name: String = line(0)
    val dataId: String = line(1)
    val point: String = line(2)
    val city: String = line(3)
    val parentCategory: String = line(4)
    val subCategory: String = line(5)
    val brand: String = line(6)
    val keyword: String = line(7)
    val comment: String = line(8)
    val price: String = line(9)
    val grade: String = line(10)
    val matchCount: String = line(11)
    val area: String = line(12)
    val leafCount: String = line(13)
    val doorCount: String = line(14)
    val parkCount: String = line(15)
    val innerCount: String = line(16)
    val buildCount: String = line(17)
    val hotCount: String = line(18)
    val hitCount: String = line(19)
    val viewCount: String = line(20)
    val sumViewOrder: String = line(21)
    var categoryScore: String = "1"
    var keywordScore: String = "1"
    var matchCountScore: String = "1"
    var gradeScore: String = "1"
    var commentScore: String = "1"
    var priceScore: String = "1"
    var areaScore: String = "1"
    var leafCountScore: String = "1"
    var doorCountScore: String = "1"
    var parkCountScore: String = "1"
    var innerCountScore: String = "1"
    var buildCountScore: String = "1"
    val uniqueKey: String = parentCategory + "-" + subCategory
    try {
      if (featureMap.contains(uniqueKey)) {
        val featureValue = featureMap(uniqueKey)
        val scopeValue: mutable.Map[String, Threshold] = featureValue.scopeValue
        categoryScore = featureValue.categoryValue
        keywordScore = getkeyWordFeatureValue(keyword, featureValue.tagValue)
        matchCountScore = fieldRank(matchCount.toInt, scopeValue("matchCount"))
        areaScore = fieldRank(area.toInt, scopeValue("area"))
        leafCountScore = fieldRank(leafCount.toInt, scopeValue("leafCount"))
        doorCountScore = fieldRank(doorCount.toInt, scopeValue("doorCount"))
        parkCountScore = fieldRank(parkCount.toInt, scopeValue("parkCount"))
        innerCountScore = fieldRank(innerCount.toInt, scopeValue("innerCount"))
        buildCountScore = fieldRank(buildCount.toInt, scopeValue("buildCount"))
        gradeScore = fieldRank(grade.toInt, scopeValue("grade"))
        commentScore = fieldRank(comment.toInt, scopeValue("comment"))
        priceScore = fieldRank(price.toInt, scopeValue("price"))
      }
      val result = (Array(name, dataId, city, parentCategory, subCategory, brand, categoryScore,
        keywordScore, matchCountScore, gradeScore, commentScore, priceScore, areaScore, leafCountScore, doorCountScore,
        parkCountScore, innerCountScore, buildCountScore, point, keyword, matchCount, grade, comment, price, area,
        leafCount, doorCount, parkCount, innerCount, buildCount, hotCount, hitCount, viewCount, sumViewOrder).mkString
      ("\t"))
      return result

    } catch {
      case ex: Exception => {
        println(dataId)
        return null
      }
    }

  }

  /**
    * rank值
    *
    * @param field
    * @return
    */
  private def fieldRank(field: Int, threshold: Threshold): String = {
    if (threshold == null || threshold.categoryThreshold == null || threshold.thresholdValue == null) {
      return "0"
    }
    val categoryThreshold: Array[Int] = threshold.categoryThreshold
    val thresholdValue: Array[Int] = threshold.thresholdValue
    var j: Int = 0

    var continue: Boolean = true

    breakable(
      while (j < categoryThreshold.length) {
        if (field < categoryThreshold(j)) {
          break()
        }
        j = j + 1
      }
    )




    return String.valueOf(thresholdValue(j))

  }

  /**
    * 关键字特征值解析映射
    *
    * @param keyword      关键字
    * @param keywordScope 定义范围值
    * @return
    */
  private def getkeyWordFeatureValue(keyword: String, keywordScope: String): String = {
    var result: String = "0"
    if (StringUtils.isBlank(keyword) || StringUtils.isBlank(keywordScope)) {
      return result
    }

    var keyWordList: List[String] = List()
    val strs: Array[String] = keyword.split(",")


    for (str <- strs) {
      if (StringUtils.isNotBlank(str)) {
        var begin: Int = 0
        var end: Int = str.length
        if (str.contains(":")) {
          begin = str.indexOf(":")
        }
        if (str.contains("$$") && str.endsWith("$$")) {
          end = str.lastIndexOf("$$")
        }
        if (begin + 1 <= end) {
          val newStr: String = str.substring(begin + 1, end)
          val b: Array[String] = newStr.split("-")
          for (str1 <- b) {
            keyWordList ::= str1
          }
        }
      }
    }
    //解析关键字区间值
    val tagValues: Array[String] = keywordScope.split("\\|")
    var tagMap: mutable.Map[String, String] = mutable.Map[String, String]()
    for (str <- tagValues) {
      val kv: Array[String] = str.split(":")
      val tags: Array[String] = kv(0).split("-")
      for (tag <- tags) {
        tagMap += (tag -> kv(1))
      }
    }

    //映射获取关键字分数


    for (kd <- keyWordList) {
      if (tagMap.contains(kd)) {
        result = tagMap(kd)
        return result
      }

    }

    return result
  }


}
