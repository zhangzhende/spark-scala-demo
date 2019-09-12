package movieAnalysis.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object MovieGroupByGenderAndAge {

  /**
    *
    * 统计某部电影【1193】观看者中男性、女性不同年龄段分别有多少人
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

//    整dataFrame

    //    元数据信息
    val schemaforusers = StructType("UserID::Gender::Age::Occupation::Zip-code"
      .split("::").map(column => StructField(column, StringType, true)))
    //    把每条数据变成以row为单位的数据
    val usersRDDRows: RDD[Row] = usersRDD
      .map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
    //    结合row和元数据信息，创建dataframe
    val usersDataFrame = spark.createDataFrame(usersRDDRows, schemaforusers)

    val schemaforratings = StructType("UserID::MovieID"
      .split("::").map(column => StructField(column, StringType, true)))
      .add("Rating", DoubleType, true)
      .add("Timestamp", StringType, true)
    val ratingsRDDRows: RDD[Row] = ratingsRDD
      .map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
    val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows, schemaforratings)

    val schemaformovies = StructType("MovieID::Title::Genres"
      .split("::")
      .map(column => StructField(column, StringType, true)))
    val moviesRDDRows: RDD[Row] = moviesRDD
      .map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim))
    val moviesDataframe = spark.createDataFrame(moviesRDDRows, schemaformovies)

//    整业务
    println("使用dataFrame完成的查询结果如下：")
    ratingsDataFrame.filter("MovieID=1193")
      .join(usersDataFrame,"UserID")
      .select("Gender","Age")
      .groupBy("Gender","Age")
      .count()
      .show(10)

    println("使用sparkSQL-以GlobalTempView完成的查询结果如下：")
    ratingsDataFrame.createGlobalTempView("ratings")
    usersDataFrame.createGlobalTempView("users")

    spark.sql("select Gender,Age ,count(*) from global_temp.users u " +
      "join global_temp.ratings r on u.UserID=r.UserID  where MovieID=1193 group by Gender ,Age ")
        .show(10)

    println("使用sparkSQL-以LocalTempView完成的查询结果如下：")
    ratingsDataFrame.createTempView("ratings")
    usersDataFrame.createTempView("users")
    spark.sql("select Gender,Age ,count(*) from users u " +
      "join ratings  r on u.UserID=r.UserID  where MovieID=1193 group by Gender ,Age ")
      .show(10)
    spark.stop
  }
}
