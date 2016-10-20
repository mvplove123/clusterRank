package cluster.model

import scala.beans.BeanProperty

/**
  * Created by admin on 2016/9/10.
  */

class Poi {


  //名称

  var name = ""
  //dataId
  var dataId = ""
  //城市
  var city = ""
  //guid
  var guid = " "
  var lat = ""
  var lng = ""
  var category = ""
  var subCategory = ""
  var brand = " "
  var keyword = ""
  var province = ""
  var point = ""

  //深度信息
  //酒店类评论数
  var hotelcommentnum = "0";
  //景点类评论数
  var commentcount = "0";
  //其他类评论数
  var recordCount = "0";
  //酒店价格
  var minprice = "0";
  //景点类价格
  var scenicPrice = "0";
  //其他类价格
  var sellingPrice = "0";
  var avgPrice = "0";
  //酒店星级打分
  var hotelrank = "0";
  //景点类星级打分
  var praise = "0";
  //其他类星级打分
  var scoremap = "0";

  //价格
  var price ="0";
  //档次
  var grade="0";
  //评论数
  var commentNum="0";



}
