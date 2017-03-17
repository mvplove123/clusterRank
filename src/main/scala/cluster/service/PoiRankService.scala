package cluster.service

import breeze.linalg.{DenseMatrix, DenseVector, *}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by admin on 2016/10/18.
  */
trait PoiRankService extends Serializable{



  val maxIterations = 100

  val runs = 1

  val initializationMode = "k-means||"

  def featureRank(sc: SparkContext,key:String ,features: RDD[(String, List[(String, Array[Double])])] ,
                  weight:Array[Double]):
  RDD[String]


  def featureCluster(sc: SparkContext,model: KMeansModel ,rankK: Int,key:String): mutable.Map[Int, Int] = {

    //聚类中心求和
    val sumCenterClusters: Array[Vector] = model.clusterCenters.map(x => Vectors.dense(x.toArray.sum))
    val rows: RDD[Vector] = sc.parallelize(sumCenterClusters,1)
    val rowRank: mutable.Map[Int, Int] = labelCluster(sumCenterClusters,rows,rankK, maxIterations, runs,
  initializationMode,key:String)

    return rowRank
  }


  /**
    * 业务聚类
    *
    * @param sumCenterClusters
    * @param rows
    * @param k
    * @param maxIterations
    * @param runs
    * @param initializationMode
    * @return
    */
  def labelCluster(sumCenterClusters: Array[Vector], rows: RDD[Vector], k: Int, maxIterations: Int, runs: Int,
                   initializationMode: String,key:String): mutable.Map[Int, Int] = {

    var rowRank = mutable.Map[Int, Int]()

    try{
      //二次聚类
      val model = KMeans.train(rows, k, maxIterations, runs, initializationMode)


      var lableRow: Int = 0
      var labelRowCenters = mutable.Map[Int, Int]()


      //15label 映射5label
      sumCenterClusters.foreach(
        row => {
          labelRowCenters += (lableRow -> model.predict(row))
          lableRow += 1
        }
      )

      val labelClusters = model.predict(rows)

      //rank 分类label映射
      var clusterIndex: Int = 0
      var labelClusterCenters = mutable.Map[Double, Int]()

      model.clusterCenters.foreach(
        x => {
          labelClusterCenters += (x(0) -> clusterIndex)
          clusterIndex += 1
        }
      )


      //rank 定级
      val arrayRank = model.clusterCenters.map(x => x(0))
      val rank = arrayRank.sorted(Ordering[Double].reverse)

      var rankLabel = k
      var sortRank = mutable.Map[Double, Int]()
      rank.foreach(
        x => {
          sortRank += (x -> rankLabel)
          rankLabel -= 1
        }
      )

      //分类label映射至定级
      var labelRank = mutable.Map[Int, Int]()

      labelClusterCenters.foreach(
        x => {
          labelRank += (x._2 -> sortRank.get(x._1).get)
        }
      )


      labelRowCenters.foreach(
        labelRow => {
          rowRank += (labelRow._1 -> labelRank.get(labelRow._2).get)
        }
      )
      return rowRank
    }catch {
      case ex: Exception => {

        println(key+" rank:"+ex)
        return rowRank
      }
    }
  }



  def matrixMultiply(feature:Array[Double],weight:Array[Double]): Array[Double] ={

    val dm = DenseMatrix(feature)
    val dv = DenseVector(weight)
    val matrixMultiply: DenseMatrix[Double] = dm(*, ::) :* dv
    val result: Array[Double] = matrixMultiply.toDenseVector.toArray
    return result
  }

}
