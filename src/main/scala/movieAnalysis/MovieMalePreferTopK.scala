package movieAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MovieMalePreferTopK {
  /**
    * 分别统计出男性和女性分别喜欢的电影topK，不同性别均分前K
    *
    * 思路：
    * 1.ratings中拿出（UserID::MovieID::Rating）然后转换成A（UserID，（UserID::MovieID::Rating））
    * 2.users中拿出B（UserID::Gender）
    * 3.A与Bjoin生成D (UserID，(（UserID::MovieID::Rating），Gender))
    * 4.将D中的数据按照Gender过滤出不同性别数据E
    * 5.对于E中的数据进行转换，变成F （MovieID,(Rating,1)）
    * 6.reduceByKey 得到G （MovieID，（sum(Rating),count））
    * 7.算均分 H （sum(Rating)/count,MovieID）
    * 8.逆序 J （AVG,MovieID）
    * 9.取 topK
    *
    *
    */
  Logger.getLogger("org").setLevel(Level.ERROR)
  var dataPath = "D:\\scalaWorkingSpace\\data\\ml-1m\\"
  val male = "M"
  val female = "F"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD movie user analyzer")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")
    //    UserID::Gender::Age::Occupation::Zip-code
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    //    UserID::MovieID::Rating::Timestamp
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
    //UserID::MovieID::Rating
    val ratings = ratingsRDD
      .map(_.split("::"))
      .map(item => (item(0), item(1), item(2))).cache()
    val users = usersRDD.map(_.split("::")).map(x => (x(0), x(1))).cache()
//    (UserID，(（UserID::MovieID::Rating），Gender))
    val genderRatings = ratings
      .map(item => (item._1, (item._1, item._2, item._3)))
      .join(users)
      .cache()
    println("genderRatings(UserID，(（UserID::MovieID::Rating），Gender))：")
    genderRatings.take(2).foreach(println)
    println("男性喜爱的电影前10 为：")
    val maleGenderRatings=genderRatings
      .filter(item=>item._2._2.equals(male))
//    男性数据,（MovieID,(Rating,1)）
      .map(item=>(item._2._1._2,(item._2._1._3.toDouble,1)))
      .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(item=>(item._2._1/item._2._2,item._1))
      .sortByKey(false)
      .take(10)
      .foreach(println)
    println("女性喜爱的电影前10 为：")
    val femaleGenderRatings=genderRatings
      .filter(item=>item._2._2.equals(female))
      //    女性数据,（MovieID,(Rating,1)）
      .map(item=>(item._2._1._2,(item._2._1._3.toDouble,1)))
      .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(item=>(item._2._1/item._2._2,item._1))
      .sortByKey(false)
      .take(10)
      .foreach(println)

  }

}
