package cluster.task

import cluster.utils.{Constants, GBKFileOutputFormat, WordUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable



/**
  * Created by admin on 2016/11/23.
  */
object WordSegmentorTask {


  def main(args: Array[String]) {


    val conf = new SparkConf()
    //  conf.setAppName("structure")
    //  conf.setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val wordSegmentorPercentinputPath = "/user/go2data_rank/taoyongbo/input/poiWordSegment"

    val wordSegmentorCountOutputPath = "/user/go2data_rank/taoyongbo/output/wordSegmentor/count"
    val wordCategoryCountOutputPath = "/user/go2data_rank/taoyongbo/output/wordSegmentor/cateCount"
    val wordCateTotalCountOutputPath = "/user/go2data_rank/taoyongbo/output/wordSegmentor/cateTotalCount"

    val wordSegmentorPercentOutputPath = "/user/go2data_rank/taoyongbo/output/wordSegmentor/percent"


    val countOutputPath = new Path(wordSegmentorCountOutputPath)
    val percentOutputPath = new Path(wordSegmentorPercentOutputPath)
    val categoryCountOutputPath = new Path(wordCategoryCountOutputPath)
    val cateTotalCountOutputPath = new Path(wordCateTotalCountOutputPath)

    WordUtils.delDir(sc, countOutputPath, true)
    WordUtils.delDir(sc, percentOutputPath, true)
    WordUtils.delDir(sc, categoryCountOutputPath, true)
    WordUtils.delDir(sc, cateTotalCountOutputPath, true)
    //    val outputPath = "D:\\structure\\spark\\result\\"
    //    val outputPath1 = "D:\\structure\\spark\\result1\\"
    //
    //    WordUtils.deleteLocalDir(new File(outputPath))
    //    WordUtils.deleteLocalDir(new File(outputPath1))

    //    val poiWordRdd: RDD[String] = sc.textFile("D:\\structure\\featurePoi\\test1000").cache()


    val poiWordRdd: RDD[Array[String]] = WordUtils.convert(sc, wordSegmentorPercentinputPath, Constants.gbkEncoding).map(x => x.split
    ("\t")).filter(x => (x.length == 5)&& !x(2).equals("大型商场")).cache()

    //统计term在所有类别的doc总量
    val term: RDD[(String, Int)] = poiWordRdd.flatMap(x => x(4).split("  ").distinct).map(x => (x, 1)).reduceByKey(_ + _)

    //统计term占各个类别doc数量
    val termCategory: RDD[(String, Int)] = poiWordRdd.flatMap(x => x(4).split
    ("  ").distinct.map(y => getTermPosition(x, y)))
      .reduceByKey(_ + _)


    //统计各个类别总量
    val cateCount: RDD[(String, Int)] = poiWordRdd.map(x => (x(2), 1)).reduceByKey(_ + _,1).sortByKey(true)




    //    val cateMap = cateCount.t

//    val primitiveDS = Seq(1, 2, 3).toDS()

    cateCount.saveAsNewAPIHadoopFile(wordCateTotalCountOutputPath,
      classOf[Text],
      classOf[IntWritable], classOf[GBKFileOutputFormat[Text, IntWritable]])

    val termCategoryCombine: RDD[(String, List[(String, Int)])] = termCategory.map(x => (x._1.split("-")(0), x)).combineByKey(
      (v: (String, Int)) => List(v),
      (c: List[(String, Int)], v: (String, Int)) => v :: c,
      (c1: List[(String, Int)], c2: List[(String, Int)]) => c1 ++ c2
    )

    // term占各个类别doc数量 与所有类别的doc总量 合并一行
    val result: RDD[(String, (Int, List[(String, Int)]))] = term.join(termCategoryCombine).cache()

    val allcateFix = allcatefix()

    //key　term，value:count,分类占比
//    val countResult: RDD[(String, String)] = result.map(x => (x._1, statisticCount(x._2, allcatefix))).sortBy(x=>x._2
//      .split("\t")(0).toLong,false)



//    //统计各个分类细化分类后总量
//    val s = countResult.map(x => x._2.split("\t").map(x => x.toInt)).map(x => Vector(x)).reduce(_ + _).toArray
//      .mkString("\t")
//
//    sc.parallelize(List(s),1).map(x => (null, x)).saveAsNewAPIHadoopFile(wordCategoryCountOutputPath, classOf[Text],
//      classOf[IntWritable], classOf[GBKFileOutputFormat[Text, IntWritable]])


    val countResult = result.map(x => (x._1, statisticCount(x._2,allcatefix))).sortBy(x=>x._2.split("\t")(0).toLong,false)

    val percentResult = result.map(x => (x._1, statisticpercent(x._2,allcatefix))).sortBy(x=>x._2.split("\t")(0).toLong,false)


    val finalresult: RDD[(String, String)] = sc.union(countResult,percentResult).sortBy(x=>x._2.split("\t")(0).toLong,false)

//
//    percentResult.saveAsNewAPIHadoopFile(wordSegmentorPercentOutputPath, classOf[Text], classOf[IntWritable], classOf[GBKFileOutputFormat[Text, IntWritable]])
    finalresult.saveAsNewAPIHadoopFile(wordSegmentorCountOutputPath, classOf[Text], classOf[IntWritable], classOf[GBKFileOutputFormat[Text, IntWritable]])


  }


  def computeEntropy(countResult:(String, String),cateMap: collection.Map[String, Int]): Unit ={

//    val ss = cateMap.tol





  }



  def statisticpercent(value: (Int, List[(String, Int)]),allcatefix: List[String]): String = {

    val totalCount = value._1
//    val catefixPercent: String = value._2.map(x => (x._1, WordUtils.numFormat(x._2.toDouble / totalCount,4))).sortWith(_._2 > _._2)
//      .mkString("\t")


    val catefixResult: Map[String, Int] = value._2.map(x => (x._1.split("-")(1), x._2)).toMap

    var countMap = mutable.Map[String, Int]()
    allcatefix.foreach(x => {
      var count = 0
      if (catefixResult.contains(x)) {
        count = catefixResult(x)
      }
      countMap += (x -> count)
    })



    val sortCountMap = countMap.toSeq.sortWith(_._2 > _._2).map(x => WordUtils.numFormat(x._2.toDouble / totalCount,4)).mkString("\t")


    val result = Array(totalCount, sortCountMap).mkString("\t")

    return result

  }

  def statisticCount(value: (Int, List[(String, Int)]), allcatefix: List[String]): String = {

    //term 总doc数
    val totalCount = value._1

//    val catefixCount: String = value._2.sortWith(_._2 > _._2)
//      .mkString("\t")

    val catefixResult: Map[String, Int] = value._2.map(x => (x._1.split("-")(1), x._2)).toMap

    var countMap = mutable.Map[String, Int]()
    allcatefix.foreach(x => {
      var count = 0
      if (catefixResult.contains(x)) {
        count = catefixResult(x)
      }
      countMap += (x -> count)
    })

    val sortCountMap = countMap.toSeq.sortWith(_._2 > _._2).map(x => x._2).mkString("\t")
    val result = Array(totalCount, sortCountMap) mkString ("\t")



    return result


  }


  def allcatefix(): List[String] = {
    val category = List("旅游景点", "宾馆饭店", "医疗卫生", "房地产", "学校科研", "餐饮服务", "休闲娱乐", "金融银行",
      "场馆会所", "公司企业", "邮政电信", "政府机关", "汽车服务", "购物场所", "交通出行", "地名", "新闻媒体", "体育场馆", "其它")

    val fix = List("prefix", "infix", "suffix")

//    val result: List[String] = category.flatMap(x => fix.map(y => (x + "-" + y))).sorted
    return category.sorted

  }


  def getTermPosition(wordArray: Array[String], term: String): (String, Int) = {

//    val name = ChineseConverter.traditional2Simplified(wordArray(0))
    val category = wordArray(2)

    val termCate = Array(term, category).mkString("-")

//    if (name.startsWith(term)) termCatefix = Array(term, category, "prefix").mkString("-")
//    else if (name.endsWith(term)) termCatefix = Array(term, category, "suffix").mkString("-")
//    else termCatefix = Array(term, category, "infix").mkString("-")


    return (termCate, 1)

  }


}
