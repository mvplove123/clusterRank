package cluster.utils

import java.io.File
import java.text.NumberFormat

import breeze.numerics._
import net.sourceforge.pinyin4j.PinyinHelper
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.net.{URLEncoder, URLDecoder}

/**
  * Created by admin on 2016/9/10.
  */
object WordUtils {


  /**
    * convert utf-8 to target encoding
    *
    * @param sc
    * @param path
    * @return
    */
  def convert(sc: SparkContext, path: String, encoding: String): RDD[String] = {
    val line: RDD[String] = sc.newAPIHadoopFile(path, classOf[TextInputFormat], classOf[LongWritable],
      classOf[Text]).map(x => new String(x._2.getBytes, 0, x._2.getLength, encoding))
    return line
  }


  /**
    * delete dirctionary
    *
    * @param sc
    * @param path
    * @param isRecursive
    */
  def delDir(sc: SparkContext, path: Path, isRecursive: Boolean): Unit = {
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if (hdfs.exists(path)) {
      //whether isRecursive or not
      hdfs.delete(path, isRecursive)
    }
  }


  /**
    * 删除本地文件夹及相应文件
    *
    * @param dir
    * @return
    */
  def deleteLocalDir(dir: File): Boolean = {
    if (dir.isDirectory() && dir.exists()) {
      val children: Array[String] = dir.list()
      //递归删除目录中的子目录下
      val i = 0
      for (i <- 0 until children.length) {
        val success = deleteLocalDir(new File(dir, children(i)))
        if (!success) {
          return false
        }
      }
    }
    // 目录此时为空，可以删附1�7
    return dir.delete()
  }

  /**
    * 汉字转换位汉语拼音，英文字符不变
    *
    * @param chines 汉字
    * @return 拼音
    */
  def converterToSpell(chines: String): String = {
    var pinyinName: String = ""
    val nameChar: Array[Char] = chines.toCharArray
    val defaultFormat: HanyuPinyinOutputFormat = new HanyuPinyinOutputFormat
    defaultFormat.setCaseType(HanyuPinyinCaseType.LOWERCASE)
    defaultFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE)

    for (i <- 0 until nameChar.length) {

      if (nameChar(i) > 128) {
        try {


          var tmpName = PinyinHelper.toHanyuPinyinStringArray(nameChar(i), defaultFormat)(0)

          if (tmpName.contains("u:")) {
            //fix  吕梁帄1�7
            val index = tmpName.indexOf("u:")
            var newName = tmpName.substring(0, index) + "v"
            pinyinName += newName
          } else {
            pinyinName += tmpName
          }

        }
        catch {
          case e: Exception => {
            e.printStackTrace
          }
        }
      }
      else {
        pinyinName += nameChar(i)
      }
    }



    return pinyinName
  }


  def covertNum(num: Double): Double = {
    val result = if (num == 0) 1 else num
    return log10(result)
  }


  /**
    * 百分比转换
    *
    * @param num
    * @return
    */
  def numFormat(num: Double, digit: Int): String = {
    val nt: NumberFormat = NumberFormat.getPercentInstance
    nt.setMinimumFractionDigits(digit)
    val newNum: String = nt.format(num)
    return newNum
  }

  //  def strMatch(pattern: String, text: String): Boolean = {
  //    val matches = List[Int]()
  //    val m: Int = text.length
  //    val n: Int = pattern.length
  //    val rightMostIndexes: Map[Character, Int] = preprocessForBadCharacterShift(pattern)
  //    var alignedAt: Int = 0
  //    while (alignedAt + (n - 1) < m) {
  //      {
  //        var indexInPattern: Int = n - 1
  //        while (indexInPattern >= 0) {
  //          {
  //            val indexInText: Int = alignedAt + indexInPattern
  //            val x: Char = text.charAt(indexInText)
  //            val y: Char = pattern.charAt(indexInPattern)
  //            if (indexInText >= m) break //todo: break is not supported
  //            if (x != y) {
  //              val r: Integer = rightMostIndexes.get(x)
  //              if (r == null) {
  //                alignedAt = indexInText + 1
  //              }
  //              else {
  //                val shift: Int = indexInText - (alignedAt + r)
  //                alignedAt += if (shift > 0) shift else 1
  //              }
  //              break //todo: break is not supported
  //            }
  //            else if (indexInPattern == 0) {
  //              matches.add(alignedAt)
  //              alignedAt += 1
  //            }
  //          }
  //          ({
  //            indexInPattern -= 1; indexInPattern + 1
  //          })
  //        }
  //      }
  //    }
  //    if (matches.isEmpty) {
  //      return false
  //    }
  //    else {
  //      return true
  //    }
  //  }
  //
  //  private def preprocessForBadCharacterShift(pattern: String): Map[Character, Int] = {
  //    val map: Map[Character, Int] = Map[Character, Int]()
  //    {
  //      var i: Int = pattern.length - 1
  //      while (i >= 0) {
  //        {
  //          val c: Char = pattern.charAt(i)
  //          if (!map.contains(c)) map+=(c -> i)
  //        }
  //        ({
  //          i -= 1; i + 1
  //        })
  //      }
  //    }
  //    return map
  //  }

  /**
    * 去空白及括号,特殊符号,大小写转换,转码
    * @param beforeCurText
    * @param candidate
    * @param afterCurText
    * @param source
    * @return
    */
  def formatWord(beforeCurText: String, candidate: String, afterCurText: String, source: String): String = {

    var newBeforeCurText = ""
    if(source.equals("com.baidu.BaiduMap")&& StringUtils.isNotBlank(beforeCurText) && beforeCurText.contains(" ")){
      newBeforeCurText = beforeCurText.split(" ")(0)
    }else{
      newBeforeCurText = beforeCurText
    }

    val word = newBeforeCurText + candidate + afterCurText

    var newWord = word

    try {
      if (!word.endsWith("%") && word.contains("%")) {
        newWord = decode(word, "UTF-8")
      }
    } catch {
      case ex: Exception => {
        println(newWord + "excepiton !")
      }
    }


    val regex = "\\s|！|。|，|、|：|；|？|《|》|（|）*"

    var result = newWord.toLowerCase.replaceAll(regex, "")

    var begin = 0
    var end = 0

    if (result.endsWith(")")) {
      end = result.lastIndexOf(")") + 1
      if (result.contains("(")) {
        begin = result.lastIndexOf("(")
      }
    }

    if (begin != 0 && end != 0) {
      result = result.substring(0, begin) + result.substring(end, result.length)
    }

    return result
  }

  /**
    * 编码
    *
    * @param word
    * @param enc
    * @return
    */
  def encode(word: String, enc: String): String = URLEncoder.encode(word, enc)

  /**
    * 解码
    *
    * @param word
    * @param enc
    * @return
    */
  def decode(word: String, enc: String): String = URLDecoder.decode(word, enc)

  def main(args: Array[String]) {
    var str = "%e6%85%88%e6%b9%96%e5%8c%97%e6%9d%91%e4%b9%90%e5%9b%ad%e8%b7%af";

    //    System.out.println(URLDecoder.decode(str, "UTF-8"))


//    val result = formatWord(str)
//
//    println(result)


  }


}
