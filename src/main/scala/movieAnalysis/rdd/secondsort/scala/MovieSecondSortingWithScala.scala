package movieAnalysis.secondsort.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MovieSecondSortingWithScala {

  /**
    *
    * 使用scala做电影评分的二次排序
    * 先按照时间戳然后按照评分
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val dataPath = "D:\\scalaWorkingSpace\\data\\ml-1m\\"
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("MovieAgePreferTopK").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    //    UserID::Gender::Age::Occupation::Zip-code
    val usersRDD = sc.textFile(dataPath + "users.dat")
    //    MovieID::Title::Genres
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    //    UserID::MovieID::Rating::Timestamp
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")

    val pairWithSortKey=ratingsRDD.map(line=>{
      val splited=line.split("::")
      (new ScalaSecondarySortKey(splited(3).toDouble,splited(2).toDouble),line)
    })
    pairWithSortKey.sortByKey(false)
      .map(item=>item._2)
      .take(10)
      .foreach(println)
  }
}
