package cluster.task

import java.util.regex.{Matcher, Pattern}

import cluster.model.{Poi, Query}
import cluster.utils.{RDDMultipleTextOutputFormat, Constants, GBKFileOutputFormat, WordUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.xml.sax.SAXParseException

import scala.collection.mutable
import scala.xml.{Node, NodeSeq, XML}

/**
  * Created by admin on 2017/3/2.
  */
object BrandDelveTask {

  def main(args: Array[String]) {

    System.setProperty("spark.sql.warehouse.dir", Constants.wareHouse)
    val ss = SparkSession.builder().getOrCreate()
    val sqlContext: SQLContext = ss.sqlContext
    val sc: SparkContext = ss.sparkContext
    brandDelve(sc: SparkContext, sqlContext: SQLContext)
    sc.stop()

  }

  def brandDelve(sc: SparkContext, sqlContext: SQLContext): Unit = {


    val path = new Path(Constants.queryOutputPath)
    WordUtils.delDir(sc, path, true)
    val queryLine: RDD[String] = WordUtils.convert(sc, Constants.queryInputPath, Constants.gbkEncoding)

    val queryRdd: RDD[Query] = getQueryRdd(queryLine)


    val allQuery = queryRdd.map(x=>(x))


    val result: RDD[String] = queryRdd.map(x => (x.city, x))
      .combineByKey(
        (v: Query) => List(v),
        (c: List[Query], v: Query) => v :: c,
        (c1: List[Query], c2: List[Query]) => c1 ++ c2
      ).flatMap(x => x._2.filter(x => x.pois.length > 3).map(x => queryHandle(x)).filter(x => StringUtils
      .isNotBlank(x))
      .sortBy(x => x
        .split
        ("\t")(2)))

    val cityBrandValue = result.map(x => (x, null))

//    cityBrandValue.partitionBy(new HashPartitioner(10)).saveAsHadoopFile(Constants.queryOutputPath,
//      classOf[Text],
//      classOf[IntWritable],
//      classOf[RDDMultipleTextOutputFormat])


    cityBrandValue.saveAsNewAPIHadoopFile(Constants.queryOutputPath, classOf[Text], classOf[IntWritable],
      classOf[GBKFileOutputFormat[Text,
        IntWritable]])


  }

  def queryHandle(x: Query): String = {
    val responseLen = 10

    val term = x.term
    val city = x.city
    var shortPois = List[Poi]()
    var shortPoiNames = List[String]()

    if (x.pois.length > responseLen) shortPois = x.pois.slice(0, responseLen) else shortPois = x.pois

    val nameDict = mutable.Map[String, Int]()
    shortPois.foreach(x => {

      val poiName = x.name
      val poiType = x.category.split(";")(0)
      val normalName = normalizeName(poiName)
      //标准brand名字+类别
      val key = normalName + "\t" + poiType
      if (nameDict.contains(key)) nameDict(key) += 1 else nameDict(key) = 1
      shortPoiNames = shortPoiNames.::(poiName)
    })

    val maxItem = nameDict.maxBy(x => x._2)
    val maxItemKey: String = maxItem._1
    val maxItemCount = maxItem._2
    val maxItemCategory = maxItemKey.split("\t")(0)

    if (Constants.categoryList.contains(maxItemCategory)) {
      return ""
    }
    if ((maxItemCount * 1.0 / shortPois.length) > 0.5) {
      val outLine = Array(city, term, maxItemKey).mkString("\t") + "\t" + shortPoiNames.mkString("\t")
      return outLine
    } else {
      return ""
    }
  }


  def normalizeName(name: String): String = {
    val regexName: String = "(.*)(\\(.*\\))"
    val pName: Pattern = Pattern.compile(regexName)
    val mName: Matcher = pName.matcher(name)
    if (mName.matches) {
      val currentBrand: String = mName.group(1)
      return currentBrand
    } else {
      return name
    }
  }


  def getQueryRdd(queryLine: RDD[String]): RDD[Query] = {
    try {
      val line: RDD[Node] = queryLine.flatMap { s =>
        try {
          XML.loadString(s)
        } catch {
          case ex: SAXParseException => {
            println(ex)
            None
          }
        }
      }
      val queryRdd: RDD[Query] = line.map(x => parsePoi(x))

      return queryRdd

    } catch {
      case ex: Exception => {
        println(ex)
      }
    }
    return null

  }

  def parsePoi(x: Node): Query = {

    val query = new Query
    query.term = x.\("term").text
    query.city = x.\("city").text

    query.pois = List[Poi]()

    val pois: NodeSeq = x \ "pois" \ "poi"

    for (poi <- pois.slice(0, 10)) {
      var poiobj = new Poi
      poiobj.dataId = (poi \ "poiId").text
      poiobj.name = (poi \ "poiName").text
      poiobj.city = (poi \ "poiCity").text
      poiobj.lat = (poi \ "poiLat").text
      poiobj.lng = (poi \ "poiLng").text
      poiobj.category = (poi \ "poiType").text
      query.pois = query.pois.::(poiobj)
    }
    return query
  }


}
