package dataframe

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object GetMoviesMaleAndFemaleMixed {

  /*
  *
  * dataframe的中间使用RDD以便实现更复杂的逻辑，混合编程
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
    println("dataframe的中间使用RDD以便实现更复杂的逻辑，混合编程")
    //    首先创建各种dataframe
    var schemaForUsers = StructType(
      "UserID::Gender::Age::OccupationID::Zip-code".split("::")
        .map(column => StructField(column, StringType, true)))
    val userRDDRows = userRDD.map(_.split("::")).map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
    val usersDataFrame = spark.createDataFrame(userRDDRows, schemaForUsers)
    val schemaforratings = StructType("UserID::MovieID::".split("::")
      .map(column => StructField(column, StringType, true)))
      .add("Ratings", DoubleType, true)
      .add("Timestamp", StringType, true)
    val ratingsRDDRows = ratingsRDD.map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
    val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows, schemaforratings)
    val schemaformovies = StructType("MovieID::Title::Genres".split("::")
      .map(column => StructField(column, StringType, true)))
    val moviesRDDRows = moviesRDD.map(_.split("::")).map(line => Row(line(0).trim, line(1).trim, line(2).trim))
    val moviesDataFrame = spark.createDataFrame(moviesRDDRows, schemaformovies)
    ratingsDataFrame.select("MovieID","Ratings")
        .groupBy("MovieID")
        .avg("Ratings")
        .rdd
        .map(row=>(row(1),(row(0),row(1))))
        .sortBy(_._1.toString.toDouble,false)
        .map(tuple=>tuple._2)
        .collect
        .take(10)
        .foreach(println)

    //    最后关闭SparkSession
    spark.stop
  }
}
