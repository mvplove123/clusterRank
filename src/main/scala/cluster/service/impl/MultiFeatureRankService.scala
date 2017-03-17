package cluster.service.impl

import cluster.service.PoiRankService
import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.LinkedHashMap

/**
  * Created by admin on 2016/10/18.
  */
class MultiFeatureRankService extends PoiRankService {

  def featureRank(sc: SparkContext, key: String, features: RDD[(String, List[(String, Array[Double])])], weight: Array[Double]): RDD[String]
  = {



    val orgline: RDD[(String, Array[Double])] = features.flatMap(x => x._2).cache()

    val ss = orgline.collectAsMap()





    val line: RDD[(String, Array[Double])] = orgline.map(x => (x._1, matrixMultiply(x._2, weight)))
    val featureValue: RDD[Vector] = line.map(line => Vectors.dense(line._2)).cache()
    val clusterRankK = getK(featureValue)
    val clusterK = clusterRankK._1
    val rankK = clusterRankK._2

    val bkm = new BisectingKMeans().setK(clusterK).setMaxIter(maxIterations)


    val model: KMeansModel = KMeans.train(featureValue, clusterK, maxIterations, runs, initializationMode)

    val label: RDD[Int] = model.predict(featureValue)

    val lineLabel: RDD[((String, Array[Double]), Int)] = orgline.zip(label)

    val rowRank = featureCluster(sc: SparkContext, model, rankK: Int, key: String)

    val clusterResult: RDD[String] = lineLabel.map(line => Array(line._1._1 , line._1._2.mkString("\t"),matrixMultiply(line._1._2, weight).sum,
      line._2,
      judgeRank(key, rowRank, line._2)).mkString("\t"))


    return clusterResult
  }

  def judgeRank(key: String, rowRank: mutable.Map[Int, Int], label: Int): Int = {

    if (rowRank.isEmpty) {
      println(key + " cluster faith!")
      return 1

    } else {


      if (rowRank.contains(label)) {
        return rowRank(label)
      } else {
        println(key + " label cluster faith!")
        return 1

      }


    }
  }

  def getK(featureValue: RDD[Vector]): (Int, Int) = {

    val size = featureValue.count()

    var rankK = 5
    var clusterK = size.toInt

    if (size > 2000) {
      clusterK = 20
    }
    else if (size > rankK && size < 2000) {
      clusterK = autoGetK(featureValue)
    } else {
      rankK = size.toInt
      clusterK = size.toInt
    }

    return (clusterK, rankK)
  }

  def autoGetK(featureValue: RDD[Vector]): Int = {


    val ks: Array[Int] = Array(6, 8, 10, 12, 14, 16, 18, 20)

    val clusterMap = mutable.Map[Int, Double]()

    ks.foreach(cluster => {
      val model: KMeansModel = KMeans.train(featureValue, cluster, maxIterations, runs, initializationMode)
      val ssd = model.computeCost(featureValue)
      clusterMap += cluster -> ssd
    })

    val sortClusterMap: mutable.LinkedHashMap[Int, Double] = LinkedHashMap(clusterMap.toSeq.sortWith(_._2 > _._2): _*)




    val clusterK: (Int, Double) = clusterMap.maxBy(_._2)

    return clusterK._1

  }

}
