package cluster.service.impl

import cluster.service.PoiRankService
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2016/10/18.
  */
class multiFeatureRankServiceImpl extends PoiRankService {

  def featureRank(sc: SparkContext,line: RDD[String] ,featureValue: RDD[Vector]): RDD[String] = {

    val lines: RDD[Vector] = line.map(line => Vectors.dense(line.split('\t').slice(6, line.length).map(_.toDouble))).cache()

    val model: KMeansModel = KMeans.train(featureValue, k, maxIterations, runs, initializationMode)

    val rowRank = featureCluster(sc: SparkContext,featureValue: RDD[Vector], k: Int, maxIterations: Int, runs: Int,
      initializationMode: String)


    val featureRank: RDD[String] = featureValue.map(x=>rowRank.get(model.predict(x)).get.toString)


    val clusterResult: RDD[String] = line.map(line => line.toString + "\t" + rowRank.get(model.predict(Vectors.dense(line.split('\t')
      .slice
      (6, line.length).map(_.toDouble)))).get)


    return clusterResult

  }
}
