package cluster.task

import cluster.service.impl.StructureInfoService
import cluster.service.{PoiService, StructureService, FeatureCombineService, FeatureConvertService}
import cluster.utils.{RDDMultipleTextOutputFormat, Constants, GBKFileOutputFormat, WordUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by admin on 2016/10/10.
  */
object FeatureConvertTask {


  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)

    val path = new Path(Constants.featureValueOutputPath)
    WordUtils.delDir(sc, path, true)

    val cityPath = new Path(Constants.cityFeatureValueOutputPath)
    WordUtils.delDir(sc, cityPath, true)

    val matchCountRdd: RDD[String] = WordUtils.convert(sc, Constants.matchCountInputPath, Constants.gbkEncoding)
    val searchCountRdd: RDD[String] = WordUtils.convert(sc, Constants.searchCountInputPath, Constants.gbkEncoding)
    val poiHotCountRdd: RDD[String] = WordUtils.convert(sc, Constants.poiHotCountOutputPath, Constants.gbkEncoding)
    val structureXmlRdd: RDD[String] = WordUtils.convert(sc, Constants.structureInputPath, Constants.gbkEncoding)
    val poiRdd: RDD[String] = WordUtils.convert(sc, Constants.poiOutPutPath, Constants.gbkEncoding).cache()


    //结构化数据
    val structureInfoService = new StructureInfoService
    val structureRdd = structureInfoService.StructureRDD(poiRdd, structureXmlRdd)
    val featureCombineRdd = FeatureCombineService.CombineRDD(sc, matchCountRdd, searchCountRdd, poiHotCountRdd,
      structureRdd, poiRdd)


    val featureThresholdRdd: RDD[String] = WordUtils.convert(sc, Constants.featureThresholdInputPath, Constants.gbkEncoding)
    val featureValueRdd = FeatureConvertService.FeatureValueRDD(sc, featureCombineRdd, featureThresholdRdd).cache()


    val cityFeatureValue: RDD[(String, String)] = featureValueRdd.map(x => (WordUtils.converterToSpell(x.split
    ("\t")(2)) + "-feature", x))

    cityFeatureValue.partitionBy(new HashPartitioner(1000)).saveAsHadoopFile(Constants.cityFeatureValueOutputPath,
      classOf[Text],
      classOf[IntWritable],
      classOf[RDDMultipleTextOutputFormat])

    val featureValue: RDD[(String, String)] = featureValueRdd.map(x => (WordUtils.converterToSpell(x.split
    ("\t")(2)) + "-" + WordUtils.converterToSpell(x.split("\t")(3)) + "-feature", x))

    featureValue.partitionBy(new HashPartitioner(7500)).saveAsHadoopFile(Constants.featureValueOutputPath,
      classOf[Text],
      classOf[IntWritable],
      classOf[RDDMultipleTextOutputFormat])

    sc.stop()
  }


}
