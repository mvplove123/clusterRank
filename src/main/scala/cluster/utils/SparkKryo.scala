package cluster.utils
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.serializer.KryoRegistrator
/**
  * Created by admin on 2016/11/2.
  */

//两个成员变量name和age，同时必须实现java.io.Serializable接口
class MyClass1(val name: String, val age: Int) extends java.io.Serializable {
}

//两个成员变量name和age，同时必须实现java.io.Serializable接口
class MyClass2(val name: String, val age: Int) extends java.io.Serializable {

}

//注册使用Kryo序列化的类，要求MyClass1和MyClass2必须实现java.io.Serializable
class MyKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[MyClass1]);
    kryo.register(classOf[MyClass2]);
  }
}

object SparkKryo {
  def main(args: Array[String]) {
    //设置序列化器为KryoSerializer,也可以在配置文件中进行配置
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "cluster.utils.MyKryoRegistrator")
    val conf = new SparkConf()
    conf.setAppName("SparkKryo")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(new MyClass1("Tom", 31), new MyClass1("Jack", 23), new MyClass1("Mary", 19)))
    val fileDir = "file:///d:/wordcount" + System.currentTimeMillis()
    //将rdd中的对象通过kyro进行序列化，保存到fileDir目录中
    rdd.saveAsObjectFile(fileDir)
    //读取part-00000文件中的数据，对它进行反序列化，，得到对象集合rdd1
    val rdd1 = sc.objectFile[MyClass1](fileDir + "/" + "part-00000")


    rdd1.foreachPartition(iter => {
      while (iter.hasNext) {
        val objOfMyClass1 = iter.next();
        println(objOfMyClass1.name)
      }
    })

    sc.stop
  }
}
