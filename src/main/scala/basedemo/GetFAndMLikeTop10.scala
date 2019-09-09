import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GetFAndMLikeTop10 {
  /*
  * 目标：
  * 获取最受男性喜爱的电影top10和最受女性喜爱的电影Top10
  *
  * users.dat:
  * ID、性别 (F、 M 分别表示女性、男性）、 年龄（使用 7 个年龄段标记）、职业、邮编。
  * UserID: : Gender: : Age: : Occupation: : Zip-code
  * 1::F::1::10::48067
  *
  * ratings.dat
  * 用户 ID、电影 ID、评分（满分是 5 分）和时间戳
  * UserID: : Mov工eID: :Ratin_g: :Timestamp
  * 1::1193::5::978300760
  *
  *
  * movies.dat
  * 电影 D、电影名称和电影类型。
  * MovieID: :Title: :Genres
  * 1::Toy Story (1995)::Animation|Children's|Comedy
  *
  *
  * */

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
//    取出用户编号和性别
    val usersGender=userRDD.map(_.split("::")).map(x=>(x(0),x(1)))
    val ratings = ratingsRDD.map(_.split("::"))
      .map(x => (x(0), x(1), x(2)))
      .cache()
    val movieInfo = moviesRDD.map(_.split("::"))
      .map(x => (x(0), x(1)))
      .cache()
    val genderRatings=ratings.map(x=>(x._1,(x._1,x._2,x._3))).join(usersGender).cache()
//    使用join连接ratings和users之后，对分别过滤出男性和女性的记录进行处理

    //filter过滤出男，map拿出rating数据，这里的数据结构是（string,((string,string,string),string(性别))）
    val maleFilteredRatings=genderRatings.filter(x=>x._2._2.equals("M")).map(x=>x._2._1)
    val femaleFilteredRatings=genderRatings.filter(x=>x._2._2.equals("F")).map(x=>x._2._1)
    println("最受男性喜爱的电影top10：")
    maleFilteredRatings.map(x=>(x._2,(x._3.toDouble,1)))//转换为形如（movieID，（rating，1））格式的RDD
        .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))//（movieID，（Sum(ratings),Count(ratings)））
        .map(x=>(x._1,x._2._1.toDouble/x._2._2))
        .join(movieInfo)//（movieID，（moviesName，avgRating））
        .map(x=>(x._2._1,x._2._2))
        .sortByKey(false)
        .take(10)
        .foreach(x=>println(x._2+"的评分为"+x._1))

    println("最受女性喜爱的电影top10：")
    femaleFilteredRatings.map(x=>(x._2,(x._3.toDouble,1)))//转换为形如（movieID，（rating，1））格式的RDD
      .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))//（movieID，（Sum(ratings),Count(ratings)））
      .map(x=>(x._1,x._2._1.toDouble/x._2._2))
      .join(movieInfo)//（movieID，（moviesName，avgRating））
      .map(x=>(x._2._1,x._2._2))
      .sortByKey(false)
      .take(10)
      .foreach(x=>println(x._2+"的评分为"+x._1))





    //    最后关闭SparkSession
    spark.stop
  }
}
