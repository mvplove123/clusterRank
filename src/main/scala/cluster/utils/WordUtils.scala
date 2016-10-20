package cluster.utils

import java.io.File

import net.sourceforge.pinyin4j.PinyinHelper
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
    // 目录此时为空，可以删除
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

    for(i <- 0 until nameChar.length ){

      if (nameChar(i) > 128) {
        try {


          var tmpName = PinyinHelper.toHanyuPinyinStringArray(nameChar(i), defaultFormat)(0)

          if(tmpName.contains("u:")){  //fix  吕梁市
            val index = tmpName.indexOf("u:")
            var newName = tmpName.substring(0,index)+"v"
            pinyinName += newName
          }else{
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


  def main(args: Array[String]) {

    val str = "概率"


    print(converterToSpell(str))


  }


}
