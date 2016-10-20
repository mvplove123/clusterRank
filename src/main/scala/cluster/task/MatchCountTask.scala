package cluster.task

import cluster.service.{MatchCountService, StructureService, PoiService}
import cluster.utils.{GBKFileOutputFormat, WordUtils, Constants}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by admin on 2016/9/16.
  */
object MatchCountTask {

  def main(args: Array[String]) {

    //        val structureInputPath = "D:\\structure\\spark\\qthstructure"
    val poiInputPath = "D:\\structure\\spark\\qthpoi"
    val outputPath = "D:\\structure\\spark\\result"
    val conf = new SparkConf()
    conf.setAppName("matchCount")
    conf.setMaster("local")
    val sc: SparkContext = new SparkContext(conf)




    val poiRdd: RDD[String] = sc.textFile(poiInputPath).cache()

    //    val path = new Path(outputPath)
    //    WordUtils.delDir(sc, path, true)
    //        val poiRdd: RDD[String] = WordUtils.convert(sc, Constants.poiOutPutPath, Constants.gbkEncoding)

    //    val poiRdd: RDD[String] = PoiService.getPoiRDD(sc)
    //
    //    val structureRdd: RDD[String] = WordUtils.convert(sc, Constants.structureInputPath, Constants.gbkEncoding)
    //
   MatchCountService.getMatchCountRDD(sc,poiRdd)
//    matchCount.saveAsTextFile(outputPath)


    sc.stop()


  }

}
