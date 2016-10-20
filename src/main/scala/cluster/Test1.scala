package cluster

import breeze.linalg.{*, DenseMatrix, DenseVector}

/**
  * Created by admin on 2016/8/27.
  */
object Test1 {


  def main(args: Array[String]) {

    val a = Array(-3.0, -1.5, 0.0, 1.5, 3.0)

    val b = Array(-3.0, -1.5, 0.0, 1.5, 3.0)

    val x = DenseMatrix.eye[Double](5) * 0.5

    val dm = DenseMatrix((1.0,2.0,3.0),
      (4.0,5.0,6.0))
//    println(x)
    val adder: DenseVector[Double] = DenseVector(1.0, 2.0, 3.0)

    val result = DenseMatrix.zeros[Double](dm.rows,dm.cols).mapPairs({
      case ((row, col), value) => {
        value + adder(col)
      }
    })

    val dv = DenseVector(1.0, 2.0, 3.0)
    val nresult = dm(*, ::) :* dv




    val z = dm :* result

  println(nresult)
    println("ok")
    println(z)

  }

}
