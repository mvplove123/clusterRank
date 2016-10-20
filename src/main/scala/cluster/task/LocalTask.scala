package cluster.task

import cluster.service.{FeatureConvertService, PoiService, StructureService}
import cluster.utils.WordUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File

import spire.std.boolean

/**
  * Created by admin on 2016/9/19.
  */
object LocalTask {


  def main(args: Array[String]) {


    val thresholdInputPath = "D:\\structure\\spark\\poi-threshold.txt"
    val outputPath = "D:\\structure\\spark\\result"

    val featurepath = "D:\\structure\\spark\\featurepoi\\100featureutf8"

    val poiPath = "D:\\structure\\spark\\qth.xml"


    val conf = new SparkConf()
    conf.setAppName("localTask")
    conf.setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    WordUtils.deleteLocalDir(new File(outputPath))

//    val feature: RDD[String] = sc.textFile(featurepath).cache()
//    val threshold: RDD[String] = sc.textFile(thresholdInputPath).cache()

    val poi = sc.textFile(poiPath).cache()
    val poiRdd = PoiService.getPoiString(poi)



//    val structureRdd: RDD[String] = sc.textFile(structureInputPath).cache()

//    val structuresInfo = StructureService.StructureRDD(sc, poiRdd, structureRdd).filter(x=>StringUtils
//      .isNotBlank(x)).map(x=>(null,x))
    poiRdd.saveAsTextFile(outputPath)

    sc.stop()
  }





}
