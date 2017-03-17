package cluster.task

import breeze.linalg.{sum, DenseVector, *, DenseMatrix}
import breeze.numerics._
import cluster.utils.{GBKFileOutputFormat, Constants, WordUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
  * Created by admin on 2016/12/16.
  */

object TermStatisticTask {


//  val sqlContext = sc.sqlContext
//  import sqlContext.implicits._

  def main(args: Array[String]) {

    val sc = SparkSession.builder().appName("localTask").master("local").getOrCreate()

    var outpath = "D:\\structure\\spark\\result"

    val path = new Path(outpath)

    WordUtils.delDir(sc.sparkContext, path, true)


    val data: RDD[String] = WordUtils.convert(sc.sparkContext, "D:\\structure\\spark\\1000count", Constants
      .gbkEncoding)

    val categoryCount: Array[Double] = WordUtils.convert(sc.sparkContext, "D:\\structure\\spark\\cateCount",
      Constants.gbkEncoding).map(x=>x.split("\t")(1).toDouble).collect()


    val orgData  =data.map(x=>x.split("\t"))

    val indexData = orgData.map(x=>x.slice(0,2).mkString("\t"))


    orgData.map(x=>computeMi(x,categoryCount))

   val result =  orgData.map(x=>x.slice(2,21).map(_.toDouble)).map(x=>computeEntropy(x,categoryCount))




    val output = indexData.zip(result)

    output.saveAsNewAPIHadoopFile(outpath, classOf[Text], classOf[IntWritable], classOf[GBKFileOutputFormat[Text, IntWritable]])

//    var columnList = List[String]("term", "totalCount", "jtcx", "xxyl", "tycg", "gsqy", "qt", "ylws", "dm", "cghs", "xxky",
//      "bgfd", "fdc", "zfjg", "xwmt", "lyjd", "qcfw", "gwcs", "yzdx", "jryh", "cyfw")
//
//
//    val termTable = data.toDF(columnList: _*)
//    termTable.createTempView("termTable")
//
//
//    val sqlstr = "select * from termTable where term='公司' "
//    val re: DataFrame = termTable.sqlContext.sql(sqlstr)
//
//   val output: RDD[(Null, String)] =  re.rdd.map(x=>(null,x.mkString("\t")))
//
//    re.write.text("taoyongbo/output/wordSegmentor/test/")

  }


  def computeEntropy(data: Array[Double],categoryCount: Array[Double]): String ={

    val dm: DenseMatrix[Double] = DenseMatrix(data)

    val dv = DenseVector(categoryCount)
    val nresult: DenseMatrix[Double] = dm(*, ::) :/ dv
    val ss: DenseMatrix[Double] = log(dm(*, ::) :/ dv)

    println("test")

    nresult.:*=(ss)

    val outresult = nresult.map(x=>if (x.equals(NaN)) 0 else x)

    val result: String = -sum(outresult)+"\t"+outresult.toArray.mkString("\t")

    return result
  }


  def computeMi(data: Array[String],categoryCount: Array[Double]): Unit ={


      var name = data.apply(0)
      var totalCountAB = data.apply(1).toDouble

      var dataMatrix = data.slice(2,21).map(_.toDouble)










  }



}
