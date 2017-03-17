package cluster.utils

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import cluster.service.impl.SingleFeatureRankService
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2016/12/19.
  */
object MultiThreadCityRank{


  val singleFeatureRankService = new SingleFeatureRankService
  var rankRdd:RDD[(String, String)] =  null

  def cityRank(sc: SparkContext,featureSplit: RDD[(String, List[(String, Array[Double])])]): RDD[(String, String)] ={

    val pool: ExecutorService = Executors.newFixedThreadPool(20)
    rankRdd = sc.parallelize(Seq[(String, String)](), 1)


    val cityKey = featureSplit.map(x => x._1)
    val catekeys = cityKey.collect()

    catekeys.foreach(
      key => {
        val rankTask = new RankTask(key,sc,featureSplit)
        pool.execute(rankTask)
      }
    )
    pool.shutdown()

    try {
      var loop = true
      do {
        //等待所有任务完成
        loop = !pool.awaitTermination(2, TimeUnit.MILLISECONDS); //阻塞，直到线程池里所有任务结束
      } while (loop)


    } catch {
      case ex: Exception => {
        println(ex)
      }
    }
    return rankRdd
  }

   class RankTask extends Runnable with Serializable {

    var key = ""

     @transient
    var sc: SparkContext = null
    var featureSplit: RDD[(String, List[(String, Array[Double])])]=null

    def this(key: String ,sc: SparkContext,featureSplit: RDD[(String, List[(String, Array[Double])])]) {
      this()
      this.key = key
      this.sc = sc
      this.featureSplit=featureSplit
    }

    override def run(): Unit = {
      try {

        val beginTime = System.currentTimeMillis()
        val keyFeature: RDD[(String, List[(String, Array[Double])])] = featureSplit.filter(x => x._1.equals(key))
          .coalesce(1, true)
        val rankInfo: RDD[(String, String)] = singleFeatureRankService.featureRank(sc, key,keyFeature,null).map(x =>
          (key, x))
        val endTime = System.currentTimeMillis()
        println(key + " cluster has been  finished ;used time:" + (endTime - beginTime) / 1000.0 + " s")
        combineRdd(rankInfo)

      } catch {
        case ex: Exception => {
          println(key+"excepiton !"+ex)
        }
      }
    }
  }



  def combineRdd(cityRankInfo: RDD[(String, String)]): Unit ={
    this.synchronized(
      rankRdd= rankRdd.union(cityRankInfo)
    )

  }


}
