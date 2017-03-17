package cluster.service.impl

import cluster.service.PoiRankService
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by admin on 2016/10/18.
  */
class SingleFeatureRankService extends PoiRankService {
   def featureRank(sc: SparkContext,key:String ,features: RDD[(String, List[(String, Array[Double])])],weight:Array[Double]): RDD[String]
   = {

     try{
       val line: RDD[(String, Array[Double])] = features.flatMap(x=>x._2)

       val featureValue: RDD[Vector] = line.map(x=>Vectors.dense(x._2)).cache()



       val clusterRankK = getK(featureValue)
       val clusterK = clusterRankK._1
       val rankK = clusterRankK._2


       val model: KMeansModel = KMeans.train(featureValue, clusterK, maxIterations, runs, initializationMode)

       val label = model.predict(featureValue)

       val lineLabel: RDD[((String, Array[Double]), Int)] = line.zip(label)


       val rowRank: mutable.Map[Int, Int] = featureCluster(sc: SparkContext,model, rankK: Int,key:String)

       val clusterResult: RDD[String] = lineLabel.map(line => Array(line._1._1, line._2,judgeRank
       (key, rowRank, line._2)).mkString("\t"))

       return clusterResult

     } catch {
       case ex: Exception => {
         println(key +" cluster faith!")
         return sc.parallelize(Seq[String](), 1)
       }
     }

   }


  def judgeRank(key:String,rowRank: mutable.Map[Int, Int] , label:Int): Int ={

    if(rowRank.isEmpty){
      println(key +" cluster faith!")
      return 0

    }else{
      return rowRank(label)
    }
  }
  def getK(featureValue: RDD[Vector]): (Int,Int) ={

    val size = featureValue.distinct().count()
    var rankK =10
    var clusterK =size.toInt

    if(size>2000){
      clusterK = 20
    }
    else if (size>rankK && size <2000){
      clusterK = 10
    }else{
      rankK=size.toInt
      clusterK =size.toInt
    }

    return (clusterK,rankK)
  }

}
