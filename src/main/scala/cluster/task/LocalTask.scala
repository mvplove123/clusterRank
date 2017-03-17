package cluster.task

import java.io.File
import java.util.concurrent.{TimeUnit, Executors, ExecutorService}

import breeze.numerics.log10
import cluster.service.impl.{MultiFeatureRankService, SingleFeatureRankService}
import cluster.utils.{GBKFileOutputFormat, RDDMultipleTextOutputFormat, Constants, WordUtils}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.Map

/**
  * Created by admin on 2016/9/19.
  */
object LocalTask extends Serializable {

  val conf = new SparkConf()
  conf.setAppName("localTask")
  conf.setMaster("local")
  conf.set("num-executors","2")
  conf.set("executor-cores","2")
  conf.set("executor-memory","8G")

  val sc: SparkContext = new SparkContext(conf)
  var rankRdd = sc.parallelize(Seq[(String, String)](), 1)
  val outputPath = "D:\\structure\\spark\\result\\"

  def main(args: Array[String]) {

    val city = "beijingshi"

    val weightInputPath = "D:\\structure\\featurePoi\\poi-weight.txt"
    val featurepath = "D:\\structure\\featurePoi\\beijingshi-binguanfandian-feature"
    val poiPath = "D:\\structure\\spark\\qth.xml"


    WordUtils.deleteLocalDir(new File(outputPath))

    val featureValueRdd = WordUtils.convert(sc, featurepath, Constants.gbkEncoding).map(x => x.split('\t')).map(x => ((WordUtils.converterToSpell(x(2))+"-"+WordUtils.converterToSpell(x(3))), x.mkString("\t")))

    val featureSplit: RDD[(String, List[(String, Array[Double])])] = featureValueRdd.combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    ).mapValues(x => x.map(x => (x.split("\t"))).map(x => (x.mkString("\t"), x.slice(6, 18).map(_.toDouble)))).cache()

    val cityKey = featureSplit.map(x => x._1).coalesce(1, true)
    val catekeys = cityKey.collect()
    val weightRdd: Map[String, Array[Double]] = WordUtils.convert(sc, weightInputPath, Constants.gbkEncoding).map(x => x.split('\t')).map(x => ((WordUtils.converterToSpell(x(0)))
      , x.slice(2, x.length).map(_.toDouble))).collectAsMap()
    val multiFeatureRankService = new MultiFeatureRankService
    class RankTask extends Runnable with Serializable {

      var key = ""
      var city = ""
      def this(key: String,city:String) {
        this()
        this.key = key
        this.city=city
      }

      override def run(): Unit = {
        try {

          val beginTime = System.currentTimeMillis()
          val keyFeature: RDD[(String, List[(String, Array[Double])])] = featureSplit.filter(x => x._1.equals(key))
            .coalesce(1, true)
          val categoryKey = key.split("-")(1)
          val category = weightRdd(categoryKey)
          val rankInfo: RDD[(String, String)] = multiFeatureRankService.featureRank(sc, key, keyFeature, category).map(x =>
            (key, x))
          combineRdd(rankInfo,city)
          val endTime = System.currentTimeMillis()
          println(key + " cluster has been  finished ;used time:" + (endTime - beginTime) / 1000.0 + " s")

        } catch {
          case ex: Exception => {
            println(key+"excepiton !"+ex)
          }
        }
      }
    }

    val pool: ExecutorService = Executors.newFixedThreadPool(20)

    catekeys.foreach(
      key => {
        val rankTask = new RankTask(key,city)
        pool.execute(rankTask)
      }
    )
    pool.shutdown()

    try {
      var loop = true
      do {
        //等待所有任务完成
        loop = !pool.awaitTermination(2, TimeUnit.MILLISECONDS); //阻塞，直到线程池里所有任务结束
      } while (loop)


      sc.stop()
    } catch {
      case ex: Exception => {
        println(ex)
      }
    }

  }
  def combineRdd(cityRankInfo: RDD[(String, String)],city:String): Unit =  synchronized{

    cityRankInfo.saveAsHadoopFile(outputPath,
      classOf[Text],
      classOf[IntWritable],
      classOf[RDDMultipleTextOutputFormat])
  }
}
