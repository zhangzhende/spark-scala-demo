package movieAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.HashSet

object MovieAgePreferTopK {

  /**
    * 在spark中如何实现mapjoin
    * 需要借助Broadcast，把数据广播到Executor级别，让Executor上的所有任务共享该唯一的数据
    * 而不是每次运行Task的时候都要发一份数据的复制
    * 这显然降低了网络数据的传输和JVM内存的消耗
    *
    * 分别统计不同年龄段的人喜欢的电影topK
    * - Age is chosen from the following ranges:
    * *  1:  "Under 18"
    * * 18:  "18-24"
    * * 25:  "25-34"
    * * 35:  "35-44"
    * * 45:  "45-49"
    * * 50:  "50-55"
    * * 56:  "56+"
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

    val target18Users = usersRDD
      .map(_.split("::"))
      .map(item => (item(0), item(2)))
      .filter(_._2.equals("18"))
    val target25Users = usersRDD
      .map(_.split("::"))
      .map(item => (item(0), item(2)))
      .filter(item => item._2.equals("25"))

    //去除用户id
    val target18UsersSet = HashSet() ++ target18Users.map(_._1).collect()
    val target25UsersSet = HashSet() ++ target25Users.map(_._1).collect()
    //    广播数据，将用户分为两类，18的（userID）25的（userID）
    val target18UsersBroadCast = sc.broadcast(target18UsersSet)
    val target25UsersBroadCast = sc.broadcast(target25UsersSet)
    val movieIdAndNameMap = moviesRDD
      .map(_.split("::"))
      .map(item => (item(0), item(1)))
      .collect()
      .toMap
    println("年龄段为18的用户最喜爱的电影【喜欢的人数最多】topK为：")



    ratingsRDD.map(_.split("::"))
      //    UserID::MovieID
      .map(item => (item(0), item(1)))
      //      过滤出18的用户
      .filter(item => target18UsersBroadCast.value.contains(item._1))
      .map(item => (item._2, 1))
      .reduceByKey(_ + _) //同类相加
      .map(item => (item._2, item._1)) //count,movieid互换=>(count,movieid)
      .sortByKey(false) //排序，逆序
      .map(item => (item._2, item._1)) //count,movieid互换恢复=>(movieid,count)
      .take(10)
      .map(item => (movieIdAndNameMap.getOrElse(item._1, null), item._2)) //通过map从movieid拿moviename
      .foreach(println)


    println("年龄段为25的用户最喜爱的电影【喜欢的人数最多】topK为：")
    ratingsRDD.map(_.split("::"))
      //    UserID::MovieID
      .map(item => (item(0), item(1)))
      //      过滤出18的用户
      .filter(item => target25UsersBroadCast.value.contains(item._1))
      .map(item => (item._2, 1))
      .reduceByKey(_ + _) //同类相加
      .map(item => (item._2, item._1)) //count,movieid互换=>(count,movieid)
      .sortByKey(false) //排序，逆序
      .map(item => (item._2, item._1)) //count,movieid互换恢复=>(movieid,count)
      .take(10)
      .map(item => (movieIdAndNameMap.getOrElse(item._1, null), item._2)) //通过map从movieid拿moviename
      .foreach(println)

  }
}
