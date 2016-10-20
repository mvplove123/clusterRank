package cluster.service

import cluster.model.CellCut
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by admin on 2016/9/16.
  */
object MatchCountService {

  def getMatchCountRDD(sc: SparkContext, poiRdd: RDD[String]): Unit = {


    val cellCut = new CellCut(2000, 6000)

    val result = poiRdd.map(x => x.split("\t")).flatMap(
      x => {
        val name = x(0)
        val dataId = x(1)
        val point = x(2)
        val pointXY = point.split(",")
        val currentIDs: List[String] = cellCut.getCrossCell(pointXY(0), pointXY(1))
        currentIDs.map(x => (x, Array(dataId, name, point).mkString("\t")))
      }
    ).combineByKey(
      (v: String) => List(v),
      (c: List[String], v: String) => v :: c,
      (c1: List[String], c2: List[String]) => c1 ++ c2
    ).cache()


    result.map(
      x => {

        val key = x._1
        val validValues: List[Array[String]] = x._2.map(x => x.split("\t")).filter(x => {
          val point = x(2).split(",")
          val currentId = cellCut.getCurrentCell(point(0), point(1))
          return key.equals(currentId)
        })

        val invalidValues = x._2.map(x => x.split("\t")).filter(x => {
          val point = x(2).split(",")
          val currentId = cellCut.getCurrentCell(point(0), point(1))
          return !key.equals(currentId)
        })


      }
    )


    def calculate(validValues: List[Array[String]],invalidValues: List[Array[String]]): Unit ={


    }


  }
}
