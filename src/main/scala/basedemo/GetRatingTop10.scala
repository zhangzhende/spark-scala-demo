import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GetRatingTop10 {
  /*
  * 目标：
  * 计算打印出所有电影中评分最高的前10个电影名称和平均评分
  *
  * users.dat:
  * ID、性别 (F、 M 分别表示女性、男性）、 年龄（使用 7 个年龄段标记）、职业、邮编。
  * UserID: : Gender: : Age: : Occupation: : Zip-code
  * 1::F::1::10::48067
  *
  * ratings.dat
  * 用户 ID、电影 ID、评分（满分是 5 分）和时间戳
  * UserID: : Mov工eID: :Ratin_g: :T工mestamp
  * 1::1193::5::978300760
  *
  *
  * movies.dat
  * 电影 D、电影名称和电影类型。
  * MovieID: :Title: :Genres
  * 1::Toy Story (1995)::Animation|Children's|Comedy
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
    /*
    * 第一步：从ratingsRDD中取出movieID和rating，从moviesRDD中取出moviesID和Name。
    * 首先使用map算子将RDD中的每一个元素（即每行）以“：：”为分隔符进行拆分
    * 然后使用map算子从拆分后的数组中取出需要用到的元素，并将得到的RDD缓存起来
    * */
    println("所有电影中平均得分最高的是：")
    //    分割---》取前两列--》缓存
    val movieInfo = moviesRDD.map(_.split("::"))
      .map(x => (x(0), x(1)))
      .cache()
    //    分割--》取前3列--》缓存
    val ratings = ratingsRDD.map(_.split("::"))
      .map(x => (x(0), x(1), x(2)))
      .cache()

    /*
    * 第二步：
    * 从ratings的数据中使用map算子获取到形如（movieID，（rating，1））格式的RDD
    * 然后使用reduceByKey把每个电影的总评分以及点评人数算出来
    * 注意：
    * Tuple元组下标从1开始
    * list列表下标从0开始（可见上方0,1,2）
    *
    * */
    //    reduceByKey:合并具有相同键的值,例如：pairRDD={(1,2),(3,2),(1,7)} ==》{(1,9),(3,2)}
    //    这里操作的结果是获取到每个电影的总评分（分数+分数）以及人数（1+1），此时得到的RDD为（movieID，（Sum(ratings),Count(ratings)））
    val moviesAndRatings = ratings.map(x => (x._2, (x._3.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    /*
    * 第三步：
    * 把每个电影的总得分与人数相除等到平均分
    * */
    //    x的第二个属性的第一个属性值除以x的第二个属性的第一个属性值
    val avgRatings = moviesAndRatings.map(x => (x._1, x._2._1.toDouble / x._2._2))
    /*
    * 第四步：
    * 把avgRatings和moviesInfo通过关键字key连接到一起，得到形如（movieID，（moviesName，avgRating））的RDD
    * 然后格式化为（AVGRating，movieName）
    * 并按照key降序排列，取top10
    * */
//    pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
    //   * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
    avgRatings.join(movieInfo).map(x=>(x._2._1,x._2._2))
        .sortByKey(false)
        .take(10)
        .foreach(x=>println(x._2+"评分为："+x._1))

    //    最后关闭SparkSession
    spark.stop
  }

}
