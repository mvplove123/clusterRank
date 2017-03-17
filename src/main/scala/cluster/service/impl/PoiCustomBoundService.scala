package cluster.service.impl

import cluster.service.PoiBoundService
import cluster.utils.Constants
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2016/10/25.
  */
class PoiCustomBoundService extends PoiBoundService{
   def getPoiBound(args: RDD[String]*): RDD[(String, String)] = {

     val poiLine: RDD[String] = args.apply(0)
     val poiBound: RDD[(String, String)] = poiLine.map(x => getBound(x)).filter(_!=null)

     return poiBound
    }


  def getBound(poi:String): (String,String) ={

    val poiArray = poi.split('\t')

    val name: String = poiArray(0)
    val dataId: String = poiArray(1)
    val category: String = poiArray(4)
    val subCategory: String = poiArray(5)
    val point: String = poiArray(2)

    if(StringUtils.isNoneBlank(point)){
      val xy = point.split(",")


      var boundXY:String = ""

      val cateBoundKey: String = category + "-" + subCategory

      val categoryBound = Constants.getCategoryBound

      if(categoryBound.contains(cateBoundKey)){
        val dim = Constants.getCategoryBound(cateBoundKey)
          boundXY = getCrossCellXY(xy(0), xy(1),dim)
      }

      if(StringUtils.isNoneBlank(boundXY)){
        val value = Array(dataId,name,boundXY).mkString("\t")
        return (dataId,"custom_"+value)
      }

    }
    return null
  }


}
