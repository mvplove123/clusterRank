package cluster.task

import cluster.service.RankCombineService
import cluster.utils.{Constants, RDDMultipleTextOutputFormat, WordUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
/**
  * Created by admin on 2016/10/18.
  */
object RankCombineTask {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)


    val path = new Path(Constants.rankCombineOutputPath)
    WordUtils.delDir(sc, path, true)

    val multiRank: RDD[String] = WordUtils.convert(sc, Constants.allmultiRankOutputPath, Constants.gbkEncoding)

    val hotCountRank: RDD[String] = WordUtils.convert(sc, Constants.hotCountRankOutputPath, Constants.gbkEncoding)
    val hitCountRank: RDD[String] = WordUtils.convert(sc, Constants.hitCountRankOutputPath, Constants.gbkEncoding)

    val rankCombine = RankCombineService.rankCombineRDD(sc, multiRank, hotCountRank,hitCountRank).filter(StringUtils
      .isNoneBlank(_))
      .map(x => (WordUtils
      .converterToSpell(x.split
    ("\t")(2))+"-rank", x))

    rankCombine.partitionBy(new HashPartitioner(400)).saveAsHadoopFile(Constants.rankCombineOutputPath, classOf[Text],
      classOf[IntWritable],
      classOf[RDDMultipleTextOutputFormat])


    sc.stop()


  }


}
