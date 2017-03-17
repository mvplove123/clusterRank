package cluster.task

import cluster.service.{StructureService, PoiService}
import cluster.utils.{GBKFileOutputFormat, Constants, PointUtils, WordUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by admin on 2016/9/19.
  */
object HospitalCategoryTask {


  def main(args: Array[String]) {

    val hospitalOutputPath = "/user/go2data_rank/taoyongbo/output/poiName/"
            val poiInputPath = "taoyongbo/output/poi/"



    val conf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)
    val path = new Path(hospitalOutputPath)
    WordUtils.delDir(sc, path, true)


    val poiRdd: RDD[String] = WordUtils.convert(sc, Constants.poiOutPutPath, Constants.gbkEncoding)




//    val poiInfo = poiRdd.map(x=>x.split("\t")).map(x=>(WordUtils.formatWord(x(0)),Array(x(3),x(4),x(5)).mkString("\t")))
//
//    poiInfo.saveAsNewAPIHadoopFile(hospitalOutputPath, classOf[Text], classOf[IntWritable], classOf[GBKFileOutputFormat[Text, IntWritable]])
//    sc.stop()

  }


}
