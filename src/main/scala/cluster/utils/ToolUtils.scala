package cluster.utils

import java.util.regex.{Matcher, Pattern}

import cluster.task.{WordSegmentorTask, QueryLogTask}
import org.apache.commons.lang3.StringUtils

/**
  * Created by admin on 2016/10/21.
  */
object ToolUtils {


  def main(args: Array[String]) {


//    val result = WordSegmentorTask.allcatefix()
//
//    result.foreach(x=>
//      println(x)
//    )

    val key = "枣糕王五道口店"

//    var result = WordUtils.formatWord("广州 。？南站！、 (地铁站)")


    val re =  """(.*)(\\(.*\\))""".r

    val tagBrand: String = "(.*)(\\(.*\\))"

    //    val tagBrand: String = "(?<=LS:)[^$]+"

    val pTagBrand: Pattern = Pattern.compile(tagBrand)
    val mTagBrand: Matcher = pTagBrand.matcher(key)
    if (mTagBrand.matches) {
      val currentBrand: String = mTagBrand.group(1)
      println(currentBrand)
    }



  }


}
