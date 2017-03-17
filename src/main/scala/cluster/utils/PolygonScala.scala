package cluster.utils

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, Executors, ExecutorService}

import cluster.model.CellCut
import cluster.service.GpsPopularityService
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{FutureAction, SparkContext, SparkConf}

/**
  * Created by admin on 2016/10/27.
  */
object PolygonScala {

  sealed case class Loc(lat: Double, lng: Double)

  def isPointInPolygon(poly: List[Loc], x: Loc): Boolean = {
    (poly.last :: poly).sliding(2).foldLeft(false) { case (c, List(i, j)) =>
      val cond = {
        (
          (i.lat <= x.lat && x.lat < j.lat) ||
            (j.lat <= x.lat && x.lat < i.lat)
          ) &&
          (x.lng < (j.lng - i.lng) * (x.lat - i.lat) / (j.lat - i.lat) + i.lng)
      }

      if (cond) !c else c
    }
  }

  val conf = new SparkConf()
  conf.setAppName("localTask")
  conf.setMaster("local")

  val sc: SparkContext = new SparkContext(conf)

  var newrdd: RDD[Int] = sc.parallelize(Seq[Int](), 1)


  def main(args: Array[String]) {


    val seq = Seq(1,2,3,4,5)

    val rdd: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5), 1)

    val queue = new LinkedBlockingQueue[String]()


    //    val firstMappedRDD  = rdd.map { case i => println(s"firstMappedRDD  calc for $i"); i * 2 }.cache()
//
//    val secondMappedRDD = firstMappedRDD.map { case i => println(s"secondMappedRDD calc for $i"); i * 2 }
//
//    val thirdMappedRDD = secondMappedRDD.map { case i => println(s"thirdMappedRDD calc for $i"); i * 3 }


    class RankTask extends Runnable with Serializable {

      var key = 0

      def this(key: Int) {
        this()
        this.key = key
      }

      override def run(): Unit = {
        try {

          val rddsingle: RDD[Int] = rdd.map(x=>x*key)
          println(key+" MappedRDD result")
          combineRdd(rddsingle)

        } catch {
          case ex: Exception => {
            println(ex)
          }
        }
      }
    }


    val pool: ExecutorService = Executors.newFixedThreadPool(30)

    val catekeys = Array(1,2,3,4)

    catekeys.foreach(
      key => {
        val rankTask = new RankTask(key)
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


      newrdd.collect().foreach(x=>println("result"+x))
      sc.stop()
    } catch {
      case ex: Exception => {
        println(ex)
      }
    }


  }


  def combineRdd(rddsingle : RDD[Int]): Unit ={
    this.synchronized(
      newrdd = newrdd.union(rddsingle)
    )
  }



  def covert(str: String): Array[(String, String)] = {

    val strArray: Array[(String, String)] = str.split('\t').map(x => x.split(":")).map(x => (x(0) -> x(1)))
    return strArray

  }

}
