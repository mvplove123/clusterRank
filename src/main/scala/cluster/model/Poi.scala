package cluster.model

/**
  * Created by admin on 2016/9/10.
  */

class Poi extends Serializable{


  //名称
  var name = ""
  //dataId
  var dataId = ""
  //城市
  var city = ""
  //guid
  var guid = " "
  //纬度
  var lat = ""
  //经度
  var lng = ""
  //大类
  var category = ""
  //小类
  var subCategory = ""
  //品牌
  var brand = " "
  //关键字
  var keyword = ""
  //省
  var province = ""
  //坐标点
  var point = ""
  //片区
  var geometry=""

  //别名
  var alias = ""

  //高德介绍
  var introduction = ""


  //深度信息
  //酒店类评论数
  var hotelcommentnum = "0"
  //景点类评论数
  var commentcount = "0"
  //其他类评论数
  var recordCount = "0"
  //酒店价格
  var minprice = "0"
  //景点类价格
  var scenicPrice = "0"
  //其他类价格
  var sellingPrice = "0"
  var avgPrice = "0"
  //酒店星级打分
  var hotelrank = "0"
  //景点类星级打分
  var praise = "0"
  //其他类星级打分
  var scoremap = "0"
  //价格
  var price ="0"
  //档次
  var grade="0"
  //评论数
  var commentNum="0"

  //人气值
  var hotCount="0"
  //点击次数
  var hitCount="0"
  //访问次数
  var viewCount="0"
  //
  var sumViewOrder="0"


  def canEqual(other: Any): Boolean = other.isInstanceOf[Poi]

  override def equals(other: Any): Boolean = other match {
    case that: Poi =>
      (that canEqual this) &&
        dataId == that.dataId
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(dataId)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
