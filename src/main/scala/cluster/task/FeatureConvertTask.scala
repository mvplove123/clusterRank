package cluster.task

import cluster.service.{PoiService, StructureService, FeatureCombineService, FeatureConvertService}
import cluster.utils.{Constants, GBKFileOutputFormat, WordUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by admin on 2016/10/10.
  */
object FeatureConvertTask {


  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)

    val path = new Path(Constants.featureValueOutputPath)
    WordUtils.delDir(sc, path, true)



    val matchCountRdd: RDD[String] = WordUtils.convert(sc, Constants.matchCountInputPath, Constants.gbkEncoding)
    val searchCountRdd: RDD[String] = WordUtils.convert(sc, Constants.searchCountInputPath, Constants.gbkEncoding)
    val poiHotCountRdd: RDD[String] = WordUtils.convert(sc, Constants.poiHotCountInputPath, Constants.gbkEncoding)
    val structureXmlRdd: RDD[String] = WordUtils.convert(sc, Constants.structureInputPath, Constants.gbkEncoding)


        val poiRdd: RDD[String] = PoiService.getPoiRDD(sc).cache()

//    val poiRdd: RDD[String] = WordUtils.convert(sc, Constants.poiOutPutPath, Constants.gbkEncoding).cache()


    val structureRdd = StructureService.StructureRDD(sc, poiRdd, structureXmlRdd)
    val featureCombineRdd = FeatureCombineService.CombineRDD(sc, matchCountRdd, searchCountRdd, poiHotCountRdd,
      structureRdd, poiRdd)


    //    val featureCombineRdd: RDD[String] = WordUtils.convert(sc, Constants.featureCombineOutputPath, Constants.gbkEncoding)
    val featureThresholdRdd: RDD[String] = WordUtils.convert(sc, Constants.featureThresholdInputPath, Constants.gbkEncoding)

    val featureValue = FeatureConvertService.FeatureValueRDD(sc, featureCombineRdd, featureThresholdRdd).map(x => (null, x))
    featureValue.saveAsNewAPIHadoopFile(Constants.featureValueOutputPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text, IntWritable]])
    sc.stop()
  }


}
