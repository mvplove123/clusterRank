package cluster

import breeze.linalg.{*, DenseMatrix, DenseVector}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Created by admin on 2016/8/18.
  */
object Test {

  def main(args: Array[String]) {


    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val path = "D:\\structure\\featurePoi\\beijingshi-lvYouJingDian-featurePoi"

    val weightPath = "D:\\structure\\featurePoi\\poi-weight.txt"


    val weightLine = sc.textFile(weightPath).cache()



    val weightFeatureVectors: RDD[Array[String]] = weightLine.map(weightLine => weightLine.split('\t')).cache()


    var weightArray: Array[Array[String]] = weightFeatureVectors.collect()

    var weightMap = mutable.Map[String, DenseVector[Double]]()


    weightArray.foreach(
      x => {
        val key: String = x.slice(0, 1)(0)
        val value: DenseVector[Double] = DenseVector(x.slice(2, x.length).map(_.toDouble))
        weightMap += (key -> value)
      }
    )
    //
    //    val line1 = "北京凤凰岭自然风景公园-神蛙石\t1_D1000450677422\t北京市\t旅游景点\t景点\t \t2\t4\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t1.6\t0\t3"
    //
    //
    //    var sss = new String(line1.getBytes, 0, line1.length, "utf-8")
    //
    //    println(sss)


    //    val line = sc.textFile(path).map(x => new String(x.getBytes, 0, x.length, "GB18030")).map(line =>line.split('\t')
    //      .slice(6,line.length))


    val line = sc.textFile(path).cache()


    val resultfeature = line.map(line => Vectors.dense(line.split('\t').slice(6, line.length)
      .map(_
      .toDouble)))
      .cache()


    val featureVectors1: RDD[DenseMatrix[Double]] = line.map(line => DenseMatrix(line.split('\t').slice(6, line
      .length).map(_.toDouble)))
      .cache()


    featureVectors1.first()
//    val ss: DenseMatrix[Double] = featureVectors1
//
////    val matFeatureVectors1: RowMatrix = new RowMatrix(featureVectors)
//
//    println(ss.rows)


    val zhuanzhi: DenseVector[Double] = weightMap("旅游景点")


//    val matWeightFeatureVectors = ss(*, ::) :* zhuanzhi

//    val matWeightFeatureVectors: Matrix = Matrices.dense(matFeatureVectors1.numRows().toInt, 12, zhuanzhi)

//    val matFeatureVectors1Vectors: Matrix = Matrices.dense(featureVectors.count().toInt, 12, featureVectors)

    def toRDD(m: Matrix): RDD[Vector] = {
      val columns = m.toArray.grouped(m.numRows)
      val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
      val vectors = rows.map(row => Vectors.dense(row.toArray))
      sc.parallelize(vectors)
    }

//    val featureVectors1: RowMatrix = matFeatureVectors1.multiply(matWeightFeatureVectors)

//    val resultfeature = toRDD(featureVectors1.computeGramianMatrix())


    //    var resu = weightFeatureVectors*featureVectors1

    //    val test= line.map(line =>line.split('\t').slice(6,line.length))


    //    val featureVectors =line.map (line => Vectors.dense(line.split('\t').slice(6,line.length).map(_.toDouble))).cache()

    //    val featureVectors: RDD[Vector] = sc.parallelize(Seq(resu))

    var result = line.take(1)
    val numExamples = resultfeature.count()


    println(s"numExamples = $numExamples.")







    //2建立模型

    val k = 15

    val maxIterations = 20

    val runs = 2

    val initializationMode = "k-means||"

    val model = KMeans.train(resultfeature, k, maxIterations, runs, initializationMode)
    //3计算测试误差


    val cost = model.computeCost(resultfeature)

    println(s"Total cost = $cost.")


    val clusters = model.predict(resultfeature)


    //聚类中心求和
    val sumCenterClusters: Array[Vector] = model.clusterCenters.map(x => Vectors.dense(x.toArray.sum))

    val rows = sc.parallelize(sumCenterClusters)

    //二次聚类
    val model1 = KMeans.train(rows, 5, maxIterations, runs, initializationMode)



    var lableRow: Int = 0
    var labelRowCenters = mutable.Map[Int, Int]()


    //15label 映射5label
    sumCenterClusters.foreach(
      row => {
        labelRowCenters += (lableRow -> model1.predict(row))
        lableRow += 1
      }
    )

    val labelClusters = model1.predict(rows)

    //rank 分类label映射
    var clusterIndex: Int = 0
    var labelClusterCenters = mutable.Map[Double, Int]()

    model1.clusterCenters.foreach(
      x => {
        labelClusterCenters += (x(0) -> clusterIndex)
        clusterIndex += 1
      }
    )


    //rank 定级
    val arrayRank = model1.clusterCenters.map(x => x(0))
    val rank = arrayRank.sorted(Ordering[Double].reverse)

    var rankLabel = 5
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


    var rowRank = mutable.Map[Int, Int]()

    labelRowCenters.foreach(
      labelRow => {
        rowRank += (labelRow._1 -> labelRank.get(labelRow._2).get)
      }
    )


    val clusterResult = line.map(line => line.toString + "\t" + rowRank.get(model.predict(Vectors.dense(line.split('\t')
      .slice
      (6, line.length).map(_.toDouble)))).get)


    clusterResult.saveAsTextFile("D:\\structure\\featurePoi\\beijingshi-lvYouJingDian-featurePoi-vectors")

  }


}
