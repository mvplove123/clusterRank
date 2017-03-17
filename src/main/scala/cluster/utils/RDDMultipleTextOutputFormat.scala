package cluster.utils

import org.apache.hadoop.io.NullWritable

/**
  * Created by admin on 2016/10/20.
  */
class RDDMultipleTextOutputFormat extends GBKMultipleTextOutputFormat[Any, Any]  {

  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
//    key.asInstanceOf[String]
    key.asInstanceOf[String]

}
