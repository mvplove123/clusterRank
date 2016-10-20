package cluster.task

import cluster.service.RankCombineService
import cluster.utils.{Constants, RDDMultipleTextOutputFormat, WordUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
/**
  * Created by admin on 2016/10/18.
  */
object RankCombineTask {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)


    val path = new Path(Constants.rankCombineOutputPath)
    WordUtils.delDir(sc, path, true)

    val multiRank: RDD[String] = WordUtils.convert(sc, Constants.multiRankOutputPath, Constants.gbkEncoding)
    val hotCountRank: RDD[String] = WordUtils.convert(sc, Constants.hotCountRankOutputPath, Constants.gbkEncoding)

    val rankCombine = RankCombineService.rankCombineRDD(sc, multiRank, hotCountRank).map(x => (WordUtils.converterToSpell(x.split
    ("\t")(2))+"-rank", x))


//    val ss: RDD[(String, String)] = sc.parallelize(List(("w", "www"), ("b", "blog"), ("c", "com"), ("w", "bt")))
//      .map(value => (value._1, value._2 + "Test")).partitionBy(new HashPartitioner(3))

//
//      .partitionBy(new HashPartitioner(3))
//      .saveAsHadoopFile("/iteblog", classOf[String], classOf[String],
//        classOf[RDDMultipleTextOutputFormat])



    rankCombine.partitionBy(new HashPartitioner(400)).saveAsHadoopFile(Constants.rankCombineOutputPath, classOf[Text],
      classOf[IntWritable],
      classOf[RDDMultipleTextOutputFormat])


    sc.stop()


  }


}
