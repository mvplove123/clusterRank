package cluster.service

import java.util.regex.{Matcher, Pattern}

import cluster.model.Poi
import cluster.utils.{Constants, WordUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.xml.sax.SAXParseException

import scala.xml.{Elem, Node, XML}

/**
  * Created by admin on 2016/9/10.
  */
object PoiService {

  def getPoiRDD(sc: SparkContext): RDD[String] = {

    val poiNochange: RDD[String] = PoiService.getPoiString(WordUtils.convert(sc, Constants.poiXmlInputPath, Constants.gbkEncoding))

    val poiBus: RDD[String] = getPoiString(WordUtils.convert(sc, Constants.busXmlPoiPath, Constants.gbkEncoding))

    val poiMyself: RDD[String] = getPoiString(WordUtils.convert(sc, Constants.poiXmlMyselfPath, Constants.gbkEncoding))

    val poi: RDD[String] = sc.union(poiNochange, poiBus, poiMyself)

    return poi
  }

  def getPoiString(poiLine: RDD[String]): RDD[String] = {

    val poiRdd: RDD[Poi] = getPoiRdd(poiLine)
    val poi = poiRdd.map(x => Array(x.name, x.dataId, x
      .point, x
      .city,
      x.category, x.subCategory,
      x.brand, x.keyword, x.commentNum, x.price, x.grade, x.guid).mkString("\t"))
    return poi

  }

  def getPoiRdd(poiLine: RDD[String]): RDD[Poi] ={
    try {
      val line: RDD[Node] = poiLine.flatMap { s =>
        try {
          XML.loadString(s)
        } catch {
          case ex: SAXParseException => {
            println(s)
            None
          }
        }
      }
      val poiRdd: RDD[Poi] = line.map(x => parsePoi(x))
      return poiRdd

    }catch {
      case ex: Exception => {
      }
    }
    return null

  }



  def parsePoi(x: Node): Poi = {
    val poi = new Poi
    poi.name = x.\("SRC_NAME").text
    poi.city = x.\("SRC_CITY").text
    poi.dataId = "1_" + x.\("DATAID").text
    val guid = x.\("GUID").text
    if (!(guid).isEmpty) poi.guid = guid
    poi.lat = x.\("SRC_X").text
    poi.lng = x.\("SRC_Y").text
    if (!poi.lat.isEmpty) poi.point = poi.lat + "," + poi.lng

    val srcType = x.\("SRC_TYPE").text.split(";;")
    if (srcType.length > 0) {
      poi.category = srcType.apply(0)
      poi.subCategory = srcType.apply(1)
    }

    poi.keyword = x.\("KEYWORDS").text + x.\("TAG").text
    poi.province = x.\("SRC_PROVINCE").text
    poi.brand = getBrand(poi.keyword)
    poi.geometry = x.\("GEOMETRY").text
    poi.alias = x.\("SRC_ALIAS").text

    poi.introduction = x.\("INTRODUCTION").text


    val deepStr = x.\\("DEEP").text
    if (!deepStr.isEmpty) {

      try {

        val deep = "<root>" + deepStr + "</root>"

        val deepXml: Elem = XML.loadString(deep)


        val items = deepXml \ "additional" \ "data" \ "items" \ "item"

        for (item <- items) {

          val source = item.attributes("source").text

          source match {
            case "DIANPING" =>

              //其他类评论数
              val recordCount = (item \ "ReviewList" \ "RecordCount").text
              if (!(recordCount).isEmpty) poi.recordCount = featureValueToNumber(recordCount)

              //其他类价格
              val avgPrice = (item \ "Shop" \ "AvgPrice").text
              if (!(avgPrice).isEmpty) poi.avgPrice = featureValueToNumber(avgPrice)

              //其他类星级打分
              val scoremap = (item \ "Shop" \ "Scoremap").text
              if (!(scoremap).isEmpty) poi.scoremap = featureValueToNumber(scoremap)

            case "ELONG" =>
              //酒店评论数
              val hotelcommentnum = (item \ "hotelcommentnum").text
              if (!(hotelcommentnum).isEmpty) poi.hotelcommentnum = featureValueToNumber(hotelcommentnum)

              //酒店价格
              val minPrice = (item \ "minPrice" \ "hotelroomprice").text
              if (!(minPrice).isEmpty) poi.minprice = featureValueToNumber(minPrice)

              //酒店星级打分
              val hotelrank = (item \ "hotelrank").text
              if (!(hotelrank).isEmpty) poi.hotelrank = featureValueToNumber(hotelrank)

            case "TONGCHENG" =>

              //景点类评论数
              val commentcount = (item \ "commentcount").text
              if (!(commentcount).isEmpty) poi.commentcount = featureValueToNumber(commentcount)

              //景点类价格
              val scenicPrice = (item \ "price").text
              if (!(scenicPrice).isEmpty) poi.scenicPrice = featureValueToNumber(scenicPrice)

              //景点类星级打分
              val praise = (item \ "praise").text
              if (!(praise).isEmpty) poi.praise = featureValueToNumber(praise)
            case "58" =>
              //房地产价格
              val sellingPrice = (item \ "poi" \ "selling_price").text
              if (!(sellingPrice).isEmpty) poi.sellingPrice = featureValueToNumber(sellingPrice)

            case everythingElse =>
                  None
          }
        }


      } catch {
        case ex: Exception => {
          println(ex)
        }
      }

      if (poi.category.equals("宾馆饭店")) {
        poi.price = poi.minprice
        poi.grade = poi.hotelrank
        poi.commentNum = poi.hotelcommentnum
      }
      else if (poi.category.equals("旅游景点")) {
        poi.price = poi.scenicPrice
        poi.grade = poi.praise
        poi.commentNum = poi.commentcount
      }
      else if (poi.category.equals("房地产")) {
        poi.price = poi.sellingPrice
        poi.grade = poi.scoremap
        poi.commentNum = poi.recordCount
      }
      else {
        poi.price = poi.avgPrice
        poi.grade = poi.scoremap
        poi.commentNum = poi.recordCount
      }
    }
    return poi
  }

  /**
    * 提取品牌
    *
    * @param keyword
    * @return
    */
  private def getBrand(keyword: String): String = {
    if (StringUtils.isBlank(keyword)) {
      return " "
    }


    val tagBrand: String = ".*LS:(.*)"

    //    val tagBrand: String = "(?<=LS:)[^$]+"

    val pTagBrand: Pattern = Pattern.compile(tagBrand)
    val mTagBrand: Matcher = pTagBrand.matcher(keyword)
    if (mTagBrand.matches) {
      val currentBrand: String = mTagBrand.group(1)
      val end: Int = currentBrand.indexOf("$")
      val brand: String = currentBrand.substring(0, end)
      return brand
    }
    else {
      return " "
    }
  }


  private def featureValueToNumber(fieldValue: String): String = {

    var newFieldValue = "0"
    if (fieldValue.contains(".")) newFieldValue = fieldValue.substring(0, fieldValue.indexOf("."))
    else
      newFieldValue = fieldValue
    return newFieldValue

  }


  def main(args: Array[String]) {
    val keyword: String = "$$TP:TP10006$$,$$DFB:美团团购$$,$$DFB:美团团购-餐饮团购$$,$$DFB:餐饮美食$$,$$DFB:餐饮美食-快餐小吃$$,$$DG:点评美食-快餐简餐$$,$$FB:餐饮美食-快餐小吃-中式快餐-云南过桥米线-快餐$$,$$LS:云南过桥米线$$,$$RC:云南过桥米线-云南过桥米线馆-云南过桥米线坊-云南过桥米线饭庄-云南过桥米线麻辣烫-云南过桥米线家和店-云南过桥米线滇城小镇-云南过桥米线麻辣土豆粉-云南过桥米线/黄焖鸡米饭-云南过桥米线老四川麻辣烫$$,$$RC:其它快餐小吃$$"
    val brand = getBrand(keyword = keyword)

    print(brand)
  }


}
