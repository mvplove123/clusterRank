package cluster

import breeze.linalg.{sum, *, DenseMatrix, DenseVector}
import breeze.numerics.log

import scala.collection.mutable.ListBuffer

/**
  * Created by admin on 2016/8/27.
  */
object Test1 {


  def main(args: Array[String]) {


    var ssi = List(1, 2, 3,5,7,8)
    var ssii = List(5, 7)
    var test =(List(1, 3, 5, 7),List(2, 6),List(11,  31))



    var listbf = ListBuffer(1, 2, 3, 4, 5, 6, 7, 8, 9)
    listbf.remove(3)

    println(listbf)

    var i=0
    var index =0
    while(i<ssi.length){
      var s= ssi(i)
      if(ssii.contains(s)){
        ssi = ssi.take(i-1)++ssi.drop(i)
      }
      i+=1
    }






    ssi =   ssi.take(3) ++ ssi.drop(4)



    println(test)




    val a = Array(-3.0, -1.5, 0.0, 1.5, 3.0)

    val b = Array(-3.0, -1.5, 0.0, 1.5, 3.0)

    val x = DenseMatrix.eye[Double](5) * 0.5

    val dm: DenseMatrix[Double] = DenseMatrix((1.0,2.0,3.0),(2.0,4.0,6.0))
//    println(x)
//    val adder: DenseVector[Double] = DenseVector(1.0, 2.0, 3.0)
//
//    val result = DenseMatrix.zeros[Double](dm.rows,dm.cols).mapPairs({
//      case ((row, col), value) => {
//        value + adder(col)
//      }
//    })

    val dv = DenseVector(1.0, 2.0, 3.0)
    val nresult: DenseMatrix[Double] = dm(*, ::) :/ dv


    val ss: DenseMatrix[Double] = log(dm(*, ::) :/ dv)
//    val result: Array[Double] = nresult.toDenseVector.toArray



    nresult.:*=(ss)



//    val z = dm :* result


    println("ok")
//    result.foreach(print(_))
  }

}
