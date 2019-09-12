package movieAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MoviePopularity {

  /**
    * 电影流行度分析
    *
    * 统计电影中平均分最高以及观看人数最多的电影top10
    */
  Logger.getLogger("org").setLevel(Level.ERROR)
  var dataPath = "D:\\scalaWorkingSpace\\data\\ml-1m\\"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD movie user analyzer")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")

//    UserID::MovieID::Rating::Timestamp
    val ratings=ratingsRDD
      .map(_.split("::"))
      .map(item=>(item(0),item(1),item(2))).cache()
    println("观看的电影中平均分最高的top10有：")

    val avgratings=ratings
      .map(item=>(item._2,(item._3.toDouble,1)))
      .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(item=>(item._2._1.toDouble/item._2._2,item._1))
      .sortByKey(false)
      .map(item=>(item._2,item._1))
      .take(10)
      .foreach(println)

    println("查看电影人数最多的top10：")
 val popular=ratings
   .map(item=>(item._2,1))
   .reduceByKey(_+_)
   .sortByKey(false)
   .map(item=>(item._2,item._1))
   .take(10)
   .foreach(println)

  }
}
