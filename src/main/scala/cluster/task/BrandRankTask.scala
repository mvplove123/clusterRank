package cluster.task

import cluster.service.{BrandRankService, PoiService}
import cluster.utils.{GBKFileOutputFormat, WordUtils, Constants}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by admin on 2016/10/19.
  */
object BrandRankTask {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)


    val path = new Path(Constants.brandRankOutputPath)
    WordUtils.delDir(sc,path,true)

    val multiRankRdd: RDD[String] = WordUtils.convert(sc, Constants.multiRankOutputPath, Constants.gbkEncoding)

    val brandRankRdd = BrandRankService.brandRankRDD(multiRankRdd).map(x => (null, x))

    brandRankRdd.saveAsNewAPIHadoopFile(Constants.brandRankOutputPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text, IntWritable]])

    sc.stop()



  }


}
