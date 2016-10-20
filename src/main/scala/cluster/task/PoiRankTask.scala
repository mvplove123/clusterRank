package cluster.task

import cluster.service.{StructureService, PoiService}
import cluster.utils.{RDDMultipleTextOutputFormat, GBKFileOutputFormat, Constants, WordUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.lib.{MultipleTextOutputFormat, MultipleOutputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by admin on 2016/9/15.
  */
object PoiRankTask {


  def main(args: Array[String]) {

    val outputPath = "/user/go2data_rank/taoyongbo/output/multiRank/"


    val conf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)


    val path = new Path(Constants.multiRankOutputPath)
    WordUtils.delDir(sc, path, true)

    val poiRdd = PoiService.getPoiRDD(sc).cache()

    val structureRdd: RDD[String] = WordUtils.convert(sc, Constants.structureInputPath, Constants.gbkEncoding)

    val structuresInfo = StructureService.StructureRDD(sc, poiRdd, structureRdd).map(x => (null, x))


    structuresInfo.saveAsHadoopFile(Constants.multiRankOutputPath, classOf[String], classOf[String],
      classOf[RDDMultipleTextOutputFormat])

    sc.stop()

  }

}
