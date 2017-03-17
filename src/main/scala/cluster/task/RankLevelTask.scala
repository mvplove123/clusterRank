package cluster.task

import cluster.utils.{Constants, RDDMultipleTextOutputFormat, WordUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkContext}

/**
  * Created by admin on 2016/9/14.
  */
object RankLevelTask {


  def main(args: Array[String]) {

    System.setProperty("spark.sql.warehouse.dir", Constants.wareHouse)
    val ss = SparkSession.builder().getOrCreate()
    val sc: SparkContext = ss.sparkContext

    val path = new Path(Constants.multiOptimizeRankOutputPath)
    WordUtils.delDir(sc, path, true)

    val rank = WordUtils.convert(sc, Constants.rankCombineOutputPath, Constants.gbkEncoding).map(x => (x.split("\t")(5), x))


    val noBrandRank = rank.filter(x => StringUtils.isBlank(x._1)).map(x => x._2)
    val brandRank: RDD[(String, String)] = rank.filter(x => StringUtils.isNotBlank(x._1))




    val brand = WordUtils.convert(sc, Constants.brandRankOutputPath, Constants.gbkEncoding).map(x => x.split("\t"))
      .map(x => (x(0), "brand_" + x(1)))

    val newBrandRank = brandRank.union(brand).combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    ).flatMap(x => optimizeRank(x._2))

    val result = noBrandRank.union(newBrandRank).map(x => (WordUtils
      .converterToSpell(x.split
      ("\t")(2)) + "-rank", x))

    result.partitionBy(new HashPartitioner(400)).saveAsHadoopFile(Constants.multiOptimizeRankOutputPath, classOf[Text],
      classOf[IntWritable],
      classOf[RDDMultipleTextOutputFormat])

    sc.stop()

  }


  def optimizeRank(fields: List[String]): List[String] = {


    var rankFields = List[String]()

    var brandFields = 0

    fields.foreach(field => {
      if (field.startsWith("brand_")) {
        brandFields = Math.ceil(field.substring(6).toDouble).toInt
      } else {
        rankFields = rankFields.::(field)
      }
    })

    return rankFields.map(x => optimize(x, brandFields))
  }

  def optimize(line: String, brandRank: Int): String = {

    val rankLevel = 1
    val fields = line.split("\t")

    val fieldsLength = fields.length
    val currentRank = fields(fieldsLength - 5).toInt
    var optimizeRankValue = currentRank

    if (brandRank == 0) {
      return line
    }


    val value = Math.abs(currentRank - brandRank)
    if (value > rankLevel) {
      if (currentRank > brandRank) optimizeRankValue = brandRank + rankLevel else optimizeRankValue = brandRank - rankLevel
    }
    val newLine = fields.slice(0, fieldsLength - 5).mkString("\t") + "\t" + optimizeRankValue + "\t" + fields.slice(fieldsLength - 4, fieldsLength).mkString("\t")
    return newLine
  }


}
