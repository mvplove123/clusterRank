package cluster.task

import cluster.service.FeatureCombineService
import cluster.utils.{Constants, GBKFileOutputFormat, WordUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by admin on 2016/10/10.
  */
object FeatureCombineTask {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)

    val path = new Path(Constants.featureCombineOutputPath)
    WordUtils.delDir(sc, path, true)
    val matchCountRdd: RDD[String] = WordUtils.convert(sc, Constants.matchCountInputPath, Constants.gbkEncoding)
    val searchCountRdd: RDD[String] = WordUtils.convert(sc, Constants.searchCountInputPath, Constants.gbkEncoding)
    val poiHotCountRdd: RDD[String] = WordUtils.convert(sc, Constants.poiHotCountInputPath, Constants.gbkEncoding)
    val structureRdd: RDD[String] = WordUtils.convert(sc, Constants.structureOutPutPath, Constants.gbkEncoding)
    val poiRdd: RDD[String] = WordUtils.convert(sc, Constants.poiOutPutPath, Constants.gbkEncoding)

    //    val poiRdd: RDD[String] = PoiService.getPoiRDD(sc)


    val featureCombine = FeatureCombineService.CombineRDD(sc, matchCountRdd, searchCountRdd, poiHotCountRdd,
      structureRdd,
      poiRdd).map(x => (null, x))
    featureCombine.saveAsNewAPIHadoopFile(Constants.featureCombineOutputPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text, IntWritable]])
    sc.stop()


  }

}
