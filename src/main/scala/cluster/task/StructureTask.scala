package cluster.task

import cluster.service.{PoiService, StructureService}
import cluster.utils.{Constants, GBKFileOutputFormat, WordUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by admin on 2016/8/18.
  */
object StructureTask {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)
    val path = new Path(Constants.structureOutPutPath)
    WordUtils.delDir(sc, path, true)
//    val poiRdd: RDD[String] = WordUtils.convert(sc, Constants.poiOutPutPath, Constants.gbkEncoding)
    val poiRdd: RDD[String] = PoiService.getPoiRDD(sc)

    val structureRdd: RDD[String] = WordUtils.convert(sc, Constants.structureInputPath, Constants.gbkEncoding)


    val structuresInfo = StructureService.StructureRDD(sc, poiRdd, structureRdd).map(x => (null, x))
    structuresInfo.saveAsNewAPIHadoopFile(Constants.structureOutPutPath, classOf[Text], classOf[IntWritable], classOf[GBKFileOutputFormat[Text, IntWritable]])
    sc.stop()


  }

}
