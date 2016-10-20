package cluster.task

import cluster.utils.{Constants, GBKFileOutputFormat, WordUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by admin on 2016/9/14.
  */
object RankLevelTask {


  def main(args: Array[String]) {
    val rankLevelOutputPath = "/user/go2data_rank/taoyongbo/output/rankLevel/"

    val conf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)


    val path = new Path(rankLevelOutputPath)
    WordUtils.delDir(sc,path,true)

    val rankLevel: RDD[(String, Int)] = WordUtils.convert(sc,Constants.rankInputPath,Constants.gbkEncoding).map(x=>x.split("\t"))
      .map(x=>(
        Array(x(2),x(3),x(4),x(36)).mkString("\t"),1
        )).reduceByKey(_+_).sortByKey(true)

    rankLevel.saveAsNewAPIHadoopFile(rankLevelOutputPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text, IntWritable]])

    sc.stop()



  }

}
