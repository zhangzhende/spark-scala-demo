package movieAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MovieUserAnalyzer {
  /**
    *统计观看指定电影1193的钱10个用的基本信息
    *
    *
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

//    指定数据类型，这样有利于排查错误，当数据类型跟计划不一样时能看出来
    val usersBasic: RDD[(String, (String, String, String))] = usersRDD
      .map(_.split("::"))
      .map(user => (user(3), (user(0), user(1), user(2))))
    for (elem <- usersBasic.collect().take(2)) {
      println("usersBasicRDD（职业id，（用户id，性别，年龄））：" + elem)
    }

    val occupations: RDD[(String, String)] = occupationsRDD
      .map(_.split("::"))
      .map(item => (item(0), item(1)))
    for (elem <- occupations.collect().take(2)) {
      println("occupationsRDD（职业id，职业名）：" + elem)
    }

    val userInfomation: RDD[(String, ((String, String, String), String))] = usersBasic.join(occupations)
    userInfomation.cache()
    for (elem <- userInfomation.collect().take(2)) {
      println("userInfomation（职业id，(（用户id，性别，年龄）,职业名）)：" + elem)
    }
    //指定的电影1193
    val targetMovie: RDD[(String, String)] = ratingsRDD
      .map(_.split("::"))
      .map(item => (item(0), item(1)))
      .filter(_._2.equals("1193"))
    for (elem <- targetMovie.collect().take(2)) {
      println("targetMovieRDD（用户id，电影id）)：" + elem)
    }

    val targetUsers: RDD[(String, ((String, String, String), String))] = userInfomation
      .map(x => (x._2._1._1, x._2))
    for (elem <- targetUsers.collect().take(2)) {
      println("targetUsers（用户id，(（用户id，性别，年龄）,职业名）)：" + elem)
    }
    println("观看电影id为1193的电影用户信息为：用户id，性别，年龄,职业名")

    val userInfomationForSpecificMovie: RDD[(String, (String, ((String, String, String), String)))] = targetMovie
      .join(targetUsers)
    for (elem <- userInfomationForSpecificMovie.collect().take(10)) {
      println("userInfomationForSpecificMovie（用户id，(电影id,(（用户id，性别，年龄）,职业名）))：" + elem)
    }
  }

}
