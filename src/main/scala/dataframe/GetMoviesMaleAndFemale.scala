package dataframe

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object GetMoviesMaleAndFemale {

  /*
  *
  * 通过dataframe实现某部电影观看者中男性和女性不同年龄段分别有多少人
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
    println("通过dataframe实现某部电影观看者中男性和女性不同年龄段分别有多少人")
    var schemaForUsers = StructType(
      "UserID::Gender::Age::OccupationID::Zip-code".split("::")
        .map(column => StructField(column, StringType, true)))
    //    把每一条数据变成以row为单位的数据
    val userRDDRows = userRDD.map(_.split("::")).map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
    //    使用SparkSession的createDataFrame方法，结合row和StructType的元数据信息基于RDD创建dataframe，这时RD就有了元数据信息的描述
    val usersDataFrame = spark.createDataFrame(userRDDRows, schemaForUsers)

    //    也可以对StructType调用add方法来对不同的StructField赋予不同的类型
    val schemaforratings = StructType("UserID::MovieID::".split("::")
      .map(column => StructField(column, StringType, true)))
      .add("Ratings", DoubleType, true)
      .add("Timestamp", StringType, true)
    val ratingsRDDRows = ratingsRDD.map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
    val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows, schemaforratings)
    //    构建movies的dataframe
    val schemaformovies = StructType("MovieID::Title::Genres".split("::")
      .map(column => StructField(column, StringType, true)))
    val moviesRDDRows = moviesRDD.map(_.split("::")).map(line => Row(line(0).trim, line(1).trim, line(2).trim))
    val moviesDataFrame = spark.createDataFrame(moviesRDDRows, schemaformovies)
    //    通过列名MovieID为1193过滤出这不电影，列名为上面指定的
    ratingsDataFrame.filter(s"MovieID=1193")
      //    直接指定基于UserID
      .join(usersDataFrame, "UserID")
      //    通过元数据信息中的Gender和Age进行筛选
      .select("Gender", "Age")
      //      直接通过元数据中的两个字段groupby
      .groupBy("Gender", "Age")
      //      然后count统计显示统计后的前10个
      .count()
      .show(10)
    //    最后关闭SparkSession
    spark.stop
  }
}
