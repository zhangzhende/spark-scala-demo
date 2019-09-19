package ebusinessInteractivesystem

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession}


object EBUserAnalyzer {

  case class UserLog(logID: Long, userID: Long, time: String, typed: Long, consumed: Double)

  case class LogOnce(logID: Long, userID: Long, count: Long)

  case class ConsumedOnce(logID: Long, userID: Long, consumed: Double)

  def main(args: Array[String]): Unit = {

    getRasingTopNByTime(args)
  }


  /**
    * 统计特定时间段内用户访问的排名topN
    * 时间范围：2019-10-01~2019-11-01
    * type=0 是访问
    * type=1是购买
    */
  def getViewTopNByTIme(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[*]"
    //    如果运行时指定参数，则用第一个参数作为spark集群的URL
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val config = new SparkConf().setMaster(masterUrl).setAppName("EB user Analyzer")
    //    sparksession 统一了spark执行的不同上下文环境，可使用这个统一编程入口来处理dataFrame和dataSet编程
    val spark = SparkSession.builder().config(config).getOrCreate()
    val sc = spark.sparkContext

    /**
      * 使用parquet方式读取用户数据
      */
    val userParquetPath = "./data/ebdata/user.parquet"
    val userInfo = spark.read.format("parquet").parquet(userParquetPath)
    val logParquetPath = "./data/ebdata/log.parquet"
    val userLog = spark.read.format("parquet").parquet(logParquetPath)

    //    TODO 可以看看functions源码
    import org.apache.spark.sql.functions._

    val startTime = "2019-10-01"
    val endTime = "2019-11-01"
    println("统计特定时间内的访问次数topN：" + startTime + "--" + endTime)
    userLog.filter("time >='" + startTime + "' and time <= '" + endTime + "' and typed = 0")
      .join(userInfo, userInfo("userID") === userLog("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(count(userInfo("userID")).alias("userLogCount"))
      .sort(desc("userLogCount"))
      .limit(5)
      .show()

    spark.close()
  }


  /**
    * 统计特定时间段内用户购买次数最多的topN
    * 时间范围：2019-10-01~2019-11-01
    * type=0 是访问
    * type=1是购买
    *
    * @param args
    */
  def getBuyTopNByTime(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[*]"
    //    如果运行时指定参数，则用第一个参数作为spark集群的URL
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val config = new SparkConf().setMaster(masterUrl).setAppName("EB user Analyzer")
    val spark = SparkSession.builder().config(config).getOrCreate()
    val sc = spark.sparkContext
    val userParquetPath = "./data/ebdata/user.parquet"
    val userInfo = spark.read.format("parquet").parquet(userParquetPath)
    val logParquetPath = "./data/ebdata/log.parquet"
    val userLog = spark.read.format("parquet").parquet(logParquetPath)
    import org.apache.spark.sql.functions._
    val startTime = "2019-10-01"
    val endTime = "2019-11-01"
    println("统计特定时间段内用户购买次数最多的topN：" + startTime + "--" + endTime)
    userLog.filter("time >='" + startTime + "' and time <= '" + endTime + "' and typed = 1")
      .join(userInfo, userInfo("userID") === userLog("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(count(userInfo("userID")).alias("totalConsumed"))
      .sort(desc("totalConsumed"))
      .limit(5)
      .show()
    spark.close()
  }


  /**
    * 统计特定时间段内访问次数增长最多的用户topN，即这周比上周访问次数增长最快的N个用户
    *
    * 基本实现思路：计算每个用户上周的访问次数，计算每个用户这周的访问次数，然后这周减上周，将差值进行排名
    * 这种思路很直接但是非常消耗性能
    *
    * 机智一点的方法：将上周用户每次访问记为-1，这周的访问访问记为1，然后在agg操作中采用sum即可巧妙的实现增长趋势变化
    *
    * @param args
    */
  def getRasingTopNByTime(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[*]"
    //    如果运行时指定参数，则用第一个参数作为spark集群的URL
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val config = new SparkConf().setMaster(masterUrl).setAppName("EB user Analyzer")
    val spark = SparkSession.builder().config(config).getOrCreate()
    val sc = spark.sparkContext
    val userParquetPath = "./data/ebdata/user.parquet"
    val userInfo = spark.read.format("parquet").parquet(userParquetPath)
    val logParquetPath = "./data/ebdata/log.parquet"
    val userLog = spark.read.format("parquet").parquet(logParquetPath)
    import org.apache.spark.sql.functions._
    val firstWeek = "2019-10-07"
    val secondWeek = "2019-10-14"
    println("统计特定时间段内访问次数增长最多的用户topN：" + firstWeek + "--" + secondWeek)
    val userLogEncoder = Encoders.product[UserLog]
    val logOnceEncoder = Encoders.product[LogOnce]
    val userLogDS = userLog.as[UserLog](userLogEncoder)
      .filter("time >= '2019-10-08' and time <='2019-10-14' and typed = '0'")
      .map(log => LogOnce(log.logID, log.userID, 1))(logOnceEncoder)
      .union(
        userLog.as[UserLog](userLogEncoder)
          .filter("time >= '2019-10-01' and time <='2019-10-07' and typed = '0'")
          .map(log => LogOnce(log.logID, log.userID, -1))(logOnceEncoder)
      )
    userLogDS.join(userInfo, userLogDS("userID") === userInfo("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(sum(userLogDS("count")).alias("viewCountIncrease"))
      .sort(desc("viewCountIncrease"))
      .limit(5)
      .show()
    spark.close()
  }

  /**
    * 统计特定时间内消费金额增长最快的topN
    * 例如这周比上周增长最快的N个用户
    *
    * @param args
    */
  def getConsumedRasingTopNByTime(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[*]"
    //    如果运行时指定参数，则用第一个参数作为spark集群的URL
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val config = new SparkConf().setMaster(masterUrl).setAppName("EB user Analyzer")
    val spark = SparkSession.builder().config(config).getOrCreate()
    val sc = spark.sparkContext
    val userParquetPath = "./data/ebdata/user.parquet"
    val userInfo = spark.read.format("parquet").parquet(userParquetPath)
    val logParquetPath = "./data/ebdata/log.parquet"
    val userLog = spark.read.format("parquet").parquet(logParquetPath)
    import org.apache.spark.sql.functions._
    val startTime = "2019-10-01"
    val endTime = "2019-10-14"
    println("统计特定时间内消费金额增长最快的topN：" + startTime + "--" + endTime)
    val userLogEncoder = Encoders.product[UserLog]
    val consumedOnceEncoder = Encoders.product[ConsumedOnce]
    val userLogConsumerDS = userLog.as[UserLog](userLogEncoder)
      .filter("time >= '2019-10-08' and time <='2019-10-14' and typed = '1'")
      .map(log => ConsumedOnce(log.userID, log.userID, log.consumed))(consumedOnceEncoder)
      .union(
        userLog.as[UserLog](userLogEncoder)
          .filter("time >= '2019-10-01' and time <='2019-10-07' and typed = '1'")
          .map(log => ConsumedOnce(log.userID, log.userID, -log.consumed))(consumedOnceEncoder)
      )
    userLogConsumerDS.join(userInfo, userLogConsumerDS("userID") === userInfo("userID"))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userLogConsumerDS("consumed")), 2).alias("consumedIncreased"))
      .sort(desc("consumedIncreased"))
      .limit(5)
      .show()
    spark.close()
  }

  /**
    * 统计注册后前两周内访问最多的N个人
    *
    * @param args
    */
  def getTopNMostViewUserAfterRegister(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[*]"
    //    如果运行时指定参数，则用第一个参数作为spark集群的URL
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val config = new SparkConf().setMaster(masterUrl).setAppName("EB user Analyzer")
    val spark = SparkSession.builder().config(config).getOrCreate()
    val sc = spark.sparkContext
    val userParquetPath = "./data/ebdata/user.parquet"
    val userInfo = spark.read.format("parquet").parquet(userParquetPath)
    val logParquetPath = "./data/ebdata/log.parquet"
    val userLog = spark.read.format("parquet").parquet(logParquetPath)
    import org.apache.spark.sql.functions._
    val startTime = "2019-10-01"
    val endTime = "2019-10-14"
    println("统计注册后前两周内访问最多的N个人：" + startTime + "--" + endTime)
    userLog.join(userInfo, userInfo("userID") === userLog("userID"))
      .filter(userInfo("registeredTime") >= "2019-10-01"
        && userInfo("registeredTime") <= "2019-10-14"
        && userLog("time") >= userInfo("registeredTime")
        && userLog("time") <= date_add(userInfo("registeredTime"), 14)
        && userLog("type") === 0
      ).groupBy(userInfo("userID"), userInfo("name"))
      .agg(count(userLog("userID")).alias("logTimes"))
      .sort(desc("logTimes"))
      .limit(10)
      .show()
    spark.close()
  }

  /**
    * 统计注册后两周内购买总额最多的人topN
    *
    * @param args
    */
  def getTopNMostBuyAfterRegister(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[*]"
    //    如果运行时指定参数，则用第一个参数作为spark集群的URL
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val config = new SparkConf().setMaster(masterUrl).setAppName("EB user Analyzer")
    val spark = SparkSession.builder().config(config).getOrCreate()
    val sc = spark.sparkContext
    val userParquetPath = "./data/ebdata/user.parquet"
    val userInfo = spark.read.format("parquet").parquet(userParquetPath)
    val logParquetPath = "./data/ebdata/log.parquet"
    val userLog = spark.read.format("parquet").parquet(logParquetPath)
    import org.apache.spark.sql.functions._
    val startTime = "2019-10-01"
    val endTime = "2019-10-14"
    println("统计注册后两周内购买总额最多的人topN：" + startTime + "--" + endTime)
    userLog.join(userInfo, userInfo("userID") === userLog("userID"))
      .filter(userInfo("registeredTime") >= "2019-10-01"
        && userInfo("registeredTime") <= "2019-10-14"
        && userLog("time") >= userInfo("registeredTime")
        && userLog("time") <= date_add(userInfo("registeredTime"), 14)
        && userLog("type") === 1
      ).groupBy(userInfo("userID"), userInfo("name"))
      .agg(round(sum(userLog("consumed")), 2).alias("totalConsumed"))
      .sort(desc("totalConsumed"))
      .limit(10)
      .show()
    spark.close()
  }
}
