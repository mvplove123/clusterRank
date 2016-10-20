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

    val hospitalOutputPath = "/user/go2data_rank/taoyongbo/output/hospitalCategory/"
            val structureInputPath = "D:\\structure\\spark\\bjstructure"
            val poiInputPath = "D:\\structure\\spark\\bjpoi"
            val outputPath = "D:\\structure\\spark\\result"


    val conf = new SparkConf()
//        conf.setAppName("structure")
//        conf.setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
////
//            val poiRdd: RDD[String] = sc.textFile(poiInputPath).cache()
//            val structureRdd: RDD[String] = sc.textFile(structureInputPath).cache()
    val path = new Path(hospitalOutputPath)
    WordUtils.delDir(sc, path, true)


    val poiRdd: RDD[String] = WordUtils.convert(sc, Constants.poiOutPutPath, Constants.gbkEncoding)

    val structureRdd: RDD[String] = WordUtils.convert(sc, Constants.structureInputPath, Constants.gbkEncoding)


    val structuresInfo = getStructureRDD(sc, poiRdd, structureRdd).filter(x=>StringUtils
      .isNotBlank(x)).map(x=>(null,x))

    structuresInfo.saveAsNewAPIHadoopFile(hospitalOutputPath, classOf[Text], classOf[IntWritable], classOf[GBKFileOutputFormat[Text, IntWritable]])
//        structuresInfo.saveAsTextFile(outputPath)

    sc.stop()

  }


  def getStructureRDD(sc: SparkContext, poiRdd: RDD[String], structureRdd: RDD[String]): RDD[String] = {

    val poi = poiRdd.map(line => line.split('\t')
    ).map(line => (line(11), Array("poi_" + line(1), line(0), line(3), line(4), line(5)).mkString("\t"))).cache()
    //dataid,name,
    // city,category
    // subcategory,point

    val guidparentChildrenInfo = mapChildrenInfo(poi, structureRdd)



    return guidparentChildrenInfo
  }


  /**
    * mapChindInfo
    *
    * @param poi
    * @param structureLine
    */
  def mapChildrenInfo(poi: RDD[(String, String)], structureLine: RDD[String]): RDD[String] = {

    //child structure info
    val childStructure: RDD[(String, String)] = structureLine.map(line => line.split('\t')).map(line => (line(2),
      "guid_" + line(1))).cache()

    //filter poilist with children info
    val guidChilds: RDD[(String, List[String])] = childStructure.union(poi).combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    ).filter(x => x._2.size == 2)

    //(k,v)=>(guid->poi1)(guid->poi2) （父节点guid,子节点信息）
    val guidChildStructureList: RDD[(String, String)] = guidChilds.map(x => getList(x._2))

     val result = mapParentInfo(poi, guidChildStructureList)


    return result

  }

  def getList(elem: List[String]): (String, String) = {
    var key = ""
    var value = ""
    elem.foreach(
      y => if (y.startsWith("guid_")) key = y.substring(5) else value = y.substring(4)
    )
    return (key, value)
  }

  def mapParentInfo(poi: RDD[(String, String)], guidparentChildrenInfo: RDD[(String, String)]): RDD[String] = {

    //filter poilist with parent info
    val guidParent: RDD[(String, List[String])] = guidparentChildrenInfo.union(poi).combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    ).filter(x => x._2.size >= 2)


    val result: RDD[String] = guidParent.flatMap(x => structureInfo(x._2))

    return result
  }



  def structureInfo(elem: List[String]): List[String] = {
    val fiterSubCate = Map("三级医院" -> 5, "二级医院" -> 4, "一级医院" -> 3, "一般医院" -> 2, "其他" -> 1)

    var parentInfo = ""
    var childsInfos = List[String]()

    elem.foreach(y => {
      if (y.startsWith("poi_"))
      {parentInfo = y.substring(4)}
      else {childsInfos::=y}
    }
    )
    val parentInfoFields: Array[String] = parentInfo.split("\t")
    val pdataId = parentInfoFields(0)
    val pname = parentInfoFields(1)
    val pcity = parentInfoFields(2)
    val pcategory = parentInfoFields(3)
    val psubCategory = parentInfoFields(4)

    var finalresult = List[String]()


    if (pcategory.equals("医疗卫生") && fiterSubCate.keySet.contains(psubCategory) ) {
      childsInfos.foreach(x=>{
        val childInfoFields: Array[String] = x.split("\t")
        val cdataId = childInfoFields(0)
        val cname = childInfoFields(1)
        val ccity = childInfoFields(2)
        val ccategory = childInfoFields(3)
        val csubCategory = childInfoFields(4)
        if(fiterSubCate.keySet.contains(csubCategory)){

          val pvalue = fiterSubCate.apply(psubCategory)
          val cvalue = fiterSubCate.apply(csubCategory)

          if (pvalue <= cvalue) {
            finalresult::=Array(parentInfo, x).mkString("\t")
          }
        }

      }
      )
    }

    finalresult.foreach(println(_))


    return finalresult
  }
}
