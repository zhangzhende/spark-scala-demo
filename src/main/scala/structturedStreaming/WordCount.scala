package structturedStreaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local[*]").setAppName("RDD movie user analyzer")
    val spark=SparkSession.builder().config(conf).appName("StructuredNetWordCount").getOrCreate()
//    引入implicits允许ScalaObject隐式转换为DataFrame
    import spark.implicits._
//    配置socket读取流配置
    val lines =spark.readStream.format("socket").option("host","localhost").option("port","9999").load()
    val words=lines.as[String].flatMap(_.split(" "))
    val wordCount=words.groupBy("value").count()
    val query=wordCount.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()

  }

}
