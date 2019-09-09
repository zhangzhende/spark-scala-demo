package basedemo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
;object SecondSort {

  /*
  * 二次排序 P27
  * 按照时间和评分降序排列
  *
  * */

  class SecondarySortKey(val first: Double, val second: Double) extends Ordered[SecondarySortKey] with Serializable {
        /**
      * 重写compare方法
      *
      * @param that
      * @return
      */
    override def compare(other: SecondarySortKey): Int = {
      if (this.first - other.first != 0) {
        //        1》2返回1,1《2 返回-1所以是降序
        (this.first - other.first).toInt
      } else {
        if (this.second - other.second > 0) {
          //          进一法，1》2时稳大于等于1
          Math.ceil(this.second - other.second).toInt
        } else if (this.second - other.second < 0) {
          //          去尾法，1《2时稳小于等于-1
          Math.floor(this.second - other.second).toInt
        } else {
          (this.second - other.second).toInt
        }
      }
    }
  }

        /**
    * 目标，二次排序，按照时间和评分降序排列
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val dataPath = "D:\\scalaWorkingSpace\\data\\ml-1m\\"
    var conf = new SparkConf().setMaster("local[*]").setAppName("RDD movie user analyzer")
    //spark2.0引入sparkSession封装了sparkContext和SQLContext，并且会在builder的getOrCreate方法中判断是否有符合要求的SparkSession存在
    //    有则使用，无则创建
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //    获取SparkSession的SparkContext
    val sc = spark.sparkContext
    //    把Spark程序运行时的日志设置为warn级别，以方便查看
    sc.setLogLevel("warn")
    //    把用到的数据加载进来转换为RDD，此时使用SC.textFile并不会读取文件，而是标记了有这个操作，遇到Action级别的算子时才会去读取文件
    val userRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")
    /*具体的处理逻辑*/
    println("对电影评分数据以timestamp和rating两个维度进行二次排序降序排列：")
    val pairWithSortKey = ratingsRDD.map(line => {
      val splited = line.split("::")
      (new SecondarySortKey(splited(3).toDouble, splited(2).toDouble), line)
    })
    //    直接调用sortByKey，此时会按照之前实现的compare方法排序
    val sorted = pairWithSortKey.sortByKey(false)
    //    按照第一属性key排序，然后取第二属性也就是一行数据
    val sortedResult = sorted.map(sortedline => sortedline._2)
    sortedResult.take(10).foreach(println)
    //    最后关闭SparkSession
    spark.stop
  }
}
