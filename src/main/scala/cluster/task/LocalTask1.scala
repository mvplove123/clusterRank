package cluster.task

import java.io.File

import cluster.service.GpsPopularityService
import cluster.utils.WordUtils
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by admin on 2016/9/19.
  */
class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[GpsPopularityService])
  }
}

object LocalTask1{



  // Make sure to set these properties *before* creating a SparkContext!
  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "cluster.task.MyRegistrator")


  val conf = new SparkConf()
  conf.setAppName("localTask")
  conf.setMaster("local")

  val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]) {


    val outputPath = "D:\\structure\\spark\\result"

    val structurepath = "D:\\structure\\spark\\bjstructure"
    val poiPath = "D:\\structure\\spark\\bjpoi"
    val gpsPath = "D:\\structure\\spark\\bjgpsutf8"
    val polygonPath = "D:\\structure\\spark\\polygonXmlutf8"


    val boundpath = "D:\\structure\\spark\\part-00027"





    WordUtils.deleteLocalDir(new File(outputPath))

    val poiRdd = sc.textFile(poiPath).cache()
    val structureRdd = sc.textFile(structurepath).cache()
    val polygonRdd = sc.textFile(polygonPath).cache()
    val boundRdd = sc.textFile(boundpath).cache()

    val gpsRdd = sc.textFile(gpsPath).cache()

    val GpsPopularityService = new GpsPopularityService
    val gpsPopularity = GpsPopularityService.gpsPopularity(sc,poiRdd,structureRdd,polygonRdd,gpsRdd).map(x=>x._2)
//    val gpsPopularity = GpsPopularityService.gpsPopularity1(sc,boundRdd,gpsRdd)


//
    gpsPopularity.saveAsTextFile(outputPath)

    sc.stop()


  }








}
