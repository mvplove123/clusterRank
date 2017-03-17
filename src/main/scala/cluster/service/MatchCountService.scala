package cluster.service

import cluster.model.{CellCut, Poi}
import cluster.utils.{Convertor_LL_Mer, Util}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2016/9/16.
  */
object MatchCountService {

   var blackSubcate = List("地铁站出入口", "公交线路")

  def getMatchCountRDD(sc: SparkContext, poiRdd: RDD[String]): Unit = {


    val cellCut = new CellCut(2000, 6000)

    val currentIDsInfo: RDD[(String, List[String])] = poiRdd.map(x => x.split("\t")).flatMap(
      x => {
        val name = x(0)
        val dataId = x(1)
        val point = x(2)
        val category = x(3)
        val city=x(4)
        val subCategory = x(4)
        val alias = x(5)
        val pointXY = point.split(",")
        val currentIDs: List[String] = cellCut.getCrossCell(pointXY(0), pointXY(1))
        currentIDs.map(x => (x, Array( dataId,name,city,point,category,subCategory,alias).mkString("\t")))
      }
    ).combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    ).cache()


    val ss = currentIDsInfo.map(
      x => {

        val key = x._1

        //有效value
        val validPoistr = x._2.filter(x => {
          val poi = x.split("\t")
          val point = poi(2).split(",")
          val currentId = cellCut.getCurrentCell(point(0), point(1))
          return key.equals(currentId)
        })

        val allPoistr: List[String] = x._2


        val validPois: List[Poi] = validPoistr.map(x=>x.split("\t")).map(x=>{

          val poi = new Poi
          poi.dataId = x(0)
          poi.name = x(0)
          poi.city = x(0)
          poi.point = x(0)
          poi.category = x(0)
          poi.subCategory = x(0)
          poi.alias = x(0)
          poi
        })

        val allPois: List[Poi] = allPoistr.map(x=>x.split("\t")).map(x=>{
          val poi = new Poi
          poi.dataId = x(0)
          poi.name = x(0)
          poi.city = x(0)
          poi.point = x(0)
          poi.category = x(0)
          poi.subCategory = x(0)
          poi.alias = x(0)
          poi
        })


//        calculate(validPois,allPois)





      }
    )


//    def calculate(validPois: List[Poi],allPois: List[Poi]): Unit ={
//
//      validPois.foreach(validPoi=>{
//        val str: StringBuilder = new StringBuilder
//        var count: Int = 0
//        allPois.foreach(poi=>{
//          if (!validPoi.city.equals(poi.city) || (validPoi.name.equals(poi.name))) {
//            continue //todo: continue is not supported
//          }
//          val d: Double = Convertor_LL_Mer.DistanceLL(validPoi.lat.toDouble, validPoi.lng.toDouble, poi.lat.toDouble, poi.lng.toDouble)
//          if (d <= 5000.0) {
//            val matches1: Boolean = Util.strMatch(validPoi.name, poi.name)
//            if (matches1 && !poi.name.endsWith(validPoi.name)) {
//              str.append(poi.name)
//              str.append(",")
//              count += 1
//              continue //todo: continue is not supported
//            }
//            if (StringUtils.isNotBlank(poi.alias)) {
//              val matches4: Boolean = Util.strMatch(poi.name, poi.alias)
//              if (matches4 && !poi.alias.endsWith(poi.name)) {
//                str.append(poi.name)
//                str.append(",")
//                count += 1
//                continue //todo: continue is not supported
//              }
//            }
//            if (StringUtils.isNotBlank(poi.alias) && !blackSubcate.contains(poi.subCategory)) {
//              val matches2: Boolean = Util.strMatch(poi.alias, poi.name)
//              if (matches2 && !poi.name.endsWith(poi.alias)) {
//                str.append(poi.name)
//                str.append(",")
//                count += 1
//                continue //todo: continue is not supported
//              }
//            }
//            if (StringUtils.isNotBlank(poi.alias) && !blackSubcate.contains(poi.subCategory) && StringUtils.isNotBlank(poi.alias)) {
//              val matches3: Boolean = Util.strMatch(poi.alias, poi.alias)
//              if (matches3 && !poi.alias.endsWith(poi.alias)) {
//                str.append(poi.name)
//                str.append(",")
//                count += 1
//                continue //todo: continue is not supported
//              }
//            }
//          }
//        })
//      })
//    }


  }
}
