package cluster.task

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import cluster.service.impl.SingleFeatureRankService
import cluster.utils.{MultiThreadCityRank, Constants, RDDMultipleTextOutputFormat, WordUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by admin on 2016/11/3.
  */
object HotCountRankTask {

  val conf = new SparkConf()
  val sc: SparkContext = new SparkContext(conf)
  var rankRdd = sc.parallelize(Seq[(String, String)](), 100)

  def main(args: Array[String]) {

    val path = new Path(Constants.hotCountRankOutputPath)
    WordUtils.delDir(sc, path, true)

    val featureValueRdd: RDD[(String, String)] = WordUtils.convert(sc, Constants.cityFeatureValueOutputPath, Constants.gbkEncoding)
      .map(x => x.split('\t')).map(x => (WordUtils.converterToSpell(x(2)), x.mkString("\t")))

    val featureSplit: RDD[(String, List[(String, Array[Double])])] = featureValueRdd.combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2, 50
    ).mapValues(x => x.map(x => (x.split("\t"))).map(x => (x(1), x.slice(30, 31).map(_.toDouble).map(x => WordUtils
      .covertNum(x))))).cache()

    val rankRdd = MultiThreadCityRank.cityRank(sc,featureSplit)

    rankRdd.saveAsHadoopFile(Constants.hotCountRankOutputPath,
      classOf[Text],
      classOf[IntWritable],
      classOf[RDDMultipleTextOutputFormat])
    sc.stop()
  }


}
