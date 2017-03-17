package cluster.task

import cluster.model.{Query, Poi}
import cluster.utils.{WordUtils, Constants}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.xml.sax.SAXParseException

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.xml.{NodeSeq, XML, Node}
import scala.util.control.Breaks._

/**
  * Created by admin on 2017/2/21.
  */
object QueryGroupTask {


  def main(args: Array[String]) {


    System.setProperty("spark.sql.warehouse.dir", Constants.wareHouse)
    val ss = SparkSession.builder().getOrCreate()
    val sqlContext: SQLContext = ss.sqlContext
    val sc: SparkContext = ss.sparkContext
    queryAnalysis(sc: SparkContext, sqlContext: SQLContext)


  }


  def queryAnalysis(sc: SparkContext, sqlContext: SQLContext): Unit = {


    val path = new Path(Constants.queryOutputPath)
    WordUtils.delDir(sc, path, true)
    val queryLine: RDD[String] = WordUtils.convert(sc, Constants.queryInputPath, Constants.gbkEncoding)

    val queryRdd = getQueryRdd(queryLine)


    val result: RDD[(String, List[mutable.Map[String, mutable.Map[Poi, Int]]])] = queryRdd.map(x => (x.city, x)).combineByKey(
      (v: Query) => List(v),
      (c: List[Query], v: Query) => v :: c,
      (c1: List[Query], c2: List[Query]) => c1 ++ c2
    ).map(x => (x._1, query_handle(x._2)))






    print(result.count())
  }



  def output(qqs:List[mutable.Map[String, mutable.Map[Poi, Int]]]): Unit ={

    var outputResult = new mutable.StringBuilder
    qqs.foreach(qqsi=>{

      qqsi.foreach(line=>{
        var lineStr = new mutable.StringBuilder
        lineStr.append(line._1)
//       val ss =  line._2.toSeq.sortBy(_._1).map(x=>(x._1.dataId+'-'+x._1.name)).toArray.mkString("\t")
//        lineStr.append(ss)
        lineStr.append('\n')

      })



    })



  }







  def query_handle(querys: List[Query]): List[mutable.Map[String, mutable.Map[Poi, Int]]] = {

    val query_dict = mutable.Map[String, mutable.Map[Poi, Int]]()
    val doc_dict = mutable.Map[Poi, List[String]]()
    for (query <- querys) {

      var pos = 0
      val term = query.term

      var short_pois = List[Poi]()
      if (query.pois.length > 5) {
        short_pois = query.pois.slice(0, 5)
      } else {
        short_pois = query.pois
      }


      val poi_dict: mutable.Map[Poi, Int] = mutable.Map[Poi, Int]()


      for (poi <- short_pois) {
        poi_dict += poi -> pos

        if (doc_dict.contains(poi)) {

          var term_array: List[String] = doc_dict(poi)
          term_array = term_array.::(term + '\t' + pos)
        } else {

          var term_array = List[String]()
          term_array = term_array.::(term + '\t' + pos)
          doc_dict += poi -> term_array
        }
        pos += 1
      }
      query_dict += term -> poi_dict
    }


    return queryLink(query_dict, doc_dict)

  }



  def queryLink(query_dict: mutable.Map[String, mutable.Map[Poi, Int]], doc_dict: mutable.Map[Poi, List[String]]): List[mutable.Map[String, mutable.Map[Poi, Int]]] = {

    var qqs = ListBuffer[mutable.Map[String, mutable.Map[Poi, Int]]]()


    query_dict.foreach(
      qi => {

        var qqsi = mutable.Map[String, mutable.Map[Poi, Int]]()
        val qiKey: String = qi._1
        val qiValue: mutable.Map[Poi, Int] = qi._2
        qqsi += qiKey -> qiValue

        qiValue.foreach(
          doc => {
            val qsdi = doc_dict(doc._1)
            qsdi.foreach(
              qj => {
                val docQuery = qj.split('\t')(0)
                if (!qqsi.contains(docQuery)) {
                  qqsi += docQuery -> query_dict(docQuery)
                }
              }
            )

          }
        )

        if (qqsi.size != 1) {
          var isUpdate = false

          var i = 0
          var index = 0
          while (i < qqs.length) {

            var crQqsi: mutable.Map[String, mutable.Map[Poi, Int]] = qqs(i)
            breakable(
              qqsi.keys.foreach(
                key => {
                  if (crQqsi.contains(key)) {
                    crQqsi = crQqsi.++(qqsi)

                    if (isUpdate) {
                      crQqsi = crQqsi.++:(qqs(index))
                      qqs.remove(index)
                      i = i - 1
                    } else {
                      isUpdate = true
                      index = i
                    }
                    break()
                  }
                }
              )
            )
            i = i + 1
          }

          if (!isUpdate) {
            qqs = qqs.+:(qqsi)

          }
        }
      }
    )



    return qqs.toList

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

    for (poi <- pois.slice(0, 5)) {
      var poiobj = new Poi
      poiobj.dataId = (poi \ "poiId").text
      poiobj.name = (poi \ "poiName").text
      poiobj.city = (poi \ "poiCity").text
      poiobj.lat = (poi \ "poiLat").text
      poiobj.lng = (poi \ "poiLng").text
      query.pois = query.pois.::(poiobj)
    }
    return query
  }
}

