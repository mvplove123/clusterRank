package cluster.task

import cluster.utils.{Constants, GBKFileOutputFormat, WordUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * Created by admin on 2016/12/2.
  */
object QueryLogTask {


  var source ="tencent"

  val imedogOrgOutputPath = "/user/go2data_rank/taoyongbo/output/imedog/org_"+source
  val imedogOutputPath = "/user/go2data_rank/taoyongbo/output/imedog/"+source
  val imedogstatic = "/user/go2data_rank/taoyongbo/output/imedogstatic/"+source



  def main(args: Array[String]) {

    System.setProperty("spark.sql.warehouse.dir", "/user/go2data_rank/taoyongbo/output/warehouse")


    val ss = SparkSession.builder().getOrCreate()
    val sqlContext: SQLContext = ss.sqlContext
    val sc: SparkContext = ss.sparkContext
    loadParseLog(sc: SparkContext, sqlContext: SQLContext)
    xuqiu1(sc,imedogOutputPath)
    //    statistic(sc,sqlContext)

  }

  def statistic(sc: SparkContext, sqlContext: SQLContext): Unit = {

    val suffixWordInputPath = "taoyongbo/input/suffixWord"
    val dimingbussinessInputPath = "/user/go2data_rank/taoyongbo/input/diming"
    val bussinessInputPath = "/user/go2data_rank/taoyongbo/input/bussiness"

    val lsbrandInputPath = "/user/go2data_rank/taoyongbo/input/lsbrand"

    val imedogOutputPath = "/user/go2data_rank/taoyongbo/output/imedog"
    val imedogOutputPath1 = "/user/go2data_rank/taoyongbo/output/imedog1"

    val suffixWordOutputPath = "/user/go2data_rank/taoyongbo/output/suffixWord"

    val imedogMatchOutputPath = "/user/go2data_rank/taoyongbo/output/imedogMatch"
    val matchWordOutputPath = "/user/go2data_rank/taoyongbo/output/matchWord"
    val noMatchWordOutputPath = "/user/go2data_rank/taoyongbo/output/imedogNoMatch"

    val imedogPath2 = new Path(imedogMatchOutputPath)
    val imedogPath3 = new Path(matchWordOutputPath)
    val nomatchPath = new Path(noMatchWordOutputPath)
    val imedogOutPath = new Path(imedogOutputPath)
    val imedogOutPath1 = new Path(imedogOutputPath1)

    WordUtils.delDir(sc, imedogOutPath, true)
    WordUtils.delDir(sc, imedogOutPath1, true)

    WordUtils.delDir(sc, imedogPath2, true)
    WordUtils.delDir(sc, imedogPath3, true)
    WordUtils.delDir(sc, nomatchPath, true)



    //后缀词加载
    val suffixWordRdd: RDD[String] = WordUtils.convert(sc, suffixWordInputPath, Constants.gbkEncoding).map(x => x.split
    ("\t")(0))

    //地名加载
    val dimingWord = WordUtils.convert(sc, dimingbussinessInputPath, Constants.gbkEncoding).collect()

    //商圈加载
    val bussinessWord = WordUtils.convert(sc, bussinessInputPath, Constants.gbkEncoding).map(x => x.split
    ("\t")(1)).collect()

    //品牌词加载
    val brandWord: collection.Map[String, Int] = WordUtils.convert(sc, lsbrandInputPath, Constants.gbkEncoding).flatMap(x => x
      .split("-|--|\t").distinct.filter(x => StringUtils.isNoneBlank(x))).map(x => (x, 1)).collectAsMap()


    val logRdd: RDD[(String, String)] = loadParseLog(sc, sqlContext).cache()

    //    //输入法日志加载
    //    val logRdd: RDD[(String, String)] = WordUtils.convert(sc, imedogOutputPath, Constants.gbkEncoding).repartition(1000).map(x => x.split("\t"))
    //      .map(x => (x(0), x
    //      (1))).cache()


    val suffixWord: Array[String] = suffixWordRdd.collect()

    val result: Double = logRdd.map(x => x._2.toInt).collect().sum

    println("total count:" + result)

    val response = logRdd.map(x => statisticNum(x, suffixWord, dimingWord,
      bussinessWord, brandWord)).cache()

    val statisticResult: RDD[(String, String)] = response.filter(x =>
      x._2.split
      ("\t")(1).toInt > 0).cache()


    val statisticNoResult: RDD[(String, String)] = response.filter(x =>
      x._2.split
      ("\t")(1).toInt == 0)


    print("match count " + statisticResult.map(x => x._2.split("\t")(0).toInt).collect().sum)
    val suffixWordStatic = statisticResult.map(x => (x._2.split("\t")(2), 1)).reduceByKey(_ + _).sortBy(x => x._2, false, 1)

    statisticResult.saveAsNewAPIHadoopFile(imedogMatchOutputPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text,
        IntWritable]])

    statisticNoResult.saveAsNewAPIHadoopFile(noMatchWordOutputPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text,
        IntWritable]])

    suffixWordStatic.saveAsNewAPIHadoopFile(matchWordOutputPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text,
        IntWritable]])


  }


  /**
    * 日志统计
    * @param sc
    */
  def xuqiu1(sc: SparkContext,imedogOutputPath:String): Unit = {


    val imedogstaticOutPath = new Path(imedogstatic)
    WordUtils.delDir(sc, imedogstaticOutPath, true)

    val result = WordUtils.convert(sc, imedogOutputPath, Constants.gbkEncoding).map(x => x.split("\t")).filter(x => x
        .length>5 &&
      StringUtils.isNoneBlank(x(4)) && x(1).length > 1).map(x => (x.slice
    (4, x.length-1).mkString("-") + "\t" + x(1), 1)).reduceByKey(_ + _).sortBy(x => x._2, false)

    result.saveAsNewAPIHadoopFile(imedogstatic, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text,
        IntWritable]])
  }


  def loadParseLog(sc: SparkContext, sqlContext: SQLContext): RDD[(String, String)] = {


    val imedog201611InputPath = "/user/go2analyzer/imedog/201611/*/"

    val imedogOutPath = new Path(imedogOutputPath)
    val imedogOrgOutPath = new Path(imedogOrgOutputPath)

    WordUtils.delDir(sc, imedogOutPath, true)
    WordUtils.delDir(sc, imedogOrgOutPath, true)

    val parquetFile = sqlContext.read.parquet(imedog201611InputPath)

    parquetFile.createOrReplaceTempView("parquetFile")

    val sqlStr = " SELECT uid,beforeCurText,candidate,afterCurText,receiveTime,reqIdx,locStr,lbs,processName FROM " +
      "parquetFile " +
      "WHERE " +
      "processName in " +
      "('com.tencent.map') order by uid,receiveTime"

    //        ,'com.baidu.BaiduMap','com.autonavi.minimap','com.sogou.map.android.maps','com.tencent.map','com.sdu.didi.psnger'

    val logfirst: DataFrame = sqlContext.sql(sqlStr)


    //原始日志格式化提取
    val out: RDD[(String, String)] = logfirst.rdd.filter(x => x.length == 9 && StringUtils.isNotBlank(x.getString(0))
      && StringUtils
      .isNotBlank(x.getString(5))).map(x => (x.getString(0), Array(WordUtils.formatWord(x.getString(1) , x.getString
    (2),x.getString(3),x.getString(8)),
      x.getLong(4), x.getString(5), x.getString(6),x.getString(7)).mkString("\t"))).cache()

    //保存原始日志
    out.saveAsNewAPIHadoopFile(imedogOrgOutputPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text,
        IntWritable]])

    //日志筛选
    val newout: RDD[(String, String)] = out.combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2, 100
    ).flatMap(x => filterValidLogs(x))

    //保存日志筛选结果
    newout.saveAsNewAPIHadoopFile(imedogOutputPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text,
        IntWritable]])







//
//    val logRdd: RDD[(String, String)] = newout.filter(x => StringUtils.isNotBlank(x._1)).map(x => (x._2.split("\t")(0), 1)).filter(x => x._1.length > 1)
//      .reduceByKey(_ + _)
//      .sortBy(x => x._2,
//        false).map(x => (x._1, x._2.toString))
//
//
//
//    logRdd.coalesce(1).saveAsNewAPIHadoopFile(imedogOutputPath1, classOf[Text], classOf[IntWritable],
//      classOf[GBKFileOutputFormat[Text,
//        IntWritable]])

    return null

  }


  def statisticNum(queryLog: (String, String), suffixWord: Array[String], dimingWord: Array[String], bussinessWord:
  Array[String], brandWord: collection.Map[String, Int]): (String,
    String) = {

    val query = queryLog._1
    val count = queryLog._2

    var matchSuffixWord = ""
    var matchCount = 0
    var matchSource = ""


    //    //匹配商圈
    //
    //      breakable(
    //        bussinessWord.foreach(x => {
    //          if (query.equals(x)) {
    //            matchSuffixWord = x
    //            matchCount = 1
    //            matchSource = "商圈"
    //            break()
    //          }
    //        })
    //      )
    //
    //
    //    //匹配地名
    //    if (matchCount == 0) {
    //      breakable(
    //        dimingWord.foreach(x => {
    //
    //          var diming = x.split("\t")
    //
    //          var province = diming(3)
    //          var city = diming(2)
    //          var town = diming(1)
    //
    //          if (query.equals(province) || query.equals(province.substring(0, province.length - 1))) {
    //            matchSuffixWord = province
    //            matchSource = "地名"
    //            matchCount = 1
    //            break()
    //          } else if (query.equals(city) || query.equals(city.substring(0, city.length - 1))) {
    //            matchSuffixWord = city
    //            matchSource = "地名"
    //            matchCount = 1
    //            break()
    //          } else if (query.equals(town) || query.equals(town.substring(0, town.length - 1))) {
    //            matchSuffixWord = town
    //            matchSource = "地名"
    //            matchCount = 1
    //            break()
    //          }
    //        })
    //      )
    //    }
    //
    //    //匹配品牌
    //    if (matchCount == 0) {
    //
    //      if (brandWord.contains(query)) {
    //        matchSource = "品牌分类"
    //        matchCount = 1
    //        matchSuffixWord = query
    //      }
    //    }
    //匹配后缀
    if (matchCount == 0) {
      breakable(
        suffixWord.foreach(x => {
          if (query.endsWith(x)) {
            matchSuffixWord = x
            matchCount = 1
            matchSource = "后缀词"
            break()
          }
        })
      )
    }
    val result = (query, Array(count, matchCount, matchSuffixWord, matchSource).mkString("\t"))
    return result

  }


  def filterValidLogs(logs: (String, List[String])): List[(String, String)] = {
    var filterLogs = List[(String, String)]()

    try {
      val orderLogs: List[String] = logs._2.sortWith((s, t) => s.split("\t")(1).toLong < t.split("\t")(1).toLong)
      val key = logs._1


      //筛选1
      var reqIdsMap = mutable.Map[String, List[String]]()
      var orgreqfirstIds = List[String]()
      var reqNofirstIds = List[String]()

      orderLogs.foreach(
        x => {
          val reqIdx = x.split("\t")(2).toLong
          if (reqIdx == 1) {

            var logResult = List[String]()
            logResult = logResult.+:(x)
            reqIdsMap += (x -> logResult)
            orgreqfirstIds = orgreqfirstIds.+:(x)
          } else {
            reqNofirstIds = reqNofirstIds.+:(x)
          }
        }
      )

      val reqfirstIds = orgreqfirstIds.sortWith((s, t) => s.split("\t")(1).toLong < t.split("\t")(1).toLong)

      if (reqfirstIds.size > 0) {

        reqNofirstIds.sortWith((s, t) => s.split("\t")(1).toLong < t.split("\t")(1).toLong).foreach(
          x => {
            var flag = false
            var lastFirstReceiveTime = reqfirstIds.apply(0).split("\t")(1).toLong
            var lastFirstKey = reqfirstIds.apply(0)
            val curEleReceiveTime = x.split("\t")(1).toLong
            val curEleKey = x

            breakable(
              //从第二个1开始
              for (i <- 1 until reqfirstIds.size) {

                val currentFirstKey = reqfirstIds.apply(i)
                val currentFirstReceiveTime = currentFirstKey.split("\t")(1).toLong
                if (Math.abs(curEleReceiveTime - lastFirstReceiveTime) > Math.abs(curEleReceiveTime - currentFirstReceiveTime)) {

                  lastFirstReceiveTime = currentFirstReceiveTime
                  lastFirstKey = currentFirstKey
                } else if (Math.abs(curEleReceiveTime - lastFirstReceiveTime) == Math.abs(curEleReceiveTime - currentFirstReceiveTime)) {

                  lastFirstReceiveTime = currentFirstReceiveTime

                  var value = reqIdsMap(lastFirstKey)
                  value = value.+:(x)
                  reqIdsMap += (lastFirstKey -> value)
                  flag = true
                } else {
                  var value = reqIdsMap(lastFirstKey)
                  value = value.+:(x)
                  reqIdsMap += (lastFirstKey -> value)
                  flag = true
                  break()
                }

              }
            )
            if (!flag) {
              var value = reqIdsMap(lastFirstKey)
              value = value.+:(x)
              reqIdsMap += (lastFirstKey -> value)
            }
          }
        )

        reqIdsMap.foreach(x => {
          val log = x._2.maxBy(x => x.split("\t")(2).toLong)
          filterLogs = filterLogs.+:(key, log)
        })

      }
      return filterLogs


    } catch {
      case ex: Exception => {
        println(logs._1 + "excepiton !" + ex)
        return filterLogs
      }
    }

    return null
  }
}
