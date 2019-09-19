package ebusinessInteractivesystem

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MakeParquet {

  def main(args: Array[String]): Unit = {
    buildParquet()
  }
  def buildParquet(): Unit = {
    val path = "./data/ebdata/"
    val masterUrl = "local[*]"
    val config = new SparkConf().setMaster(masterUrl).setAppName("EB user Analyzer")
    val spark = SparkSession.builder().config(config).getOrCreate()
    val peopleDF = spark.read.json(path + "user.json")
    peopleDF.write.parquet(path + "user.parquet")
    val logDF = spark.read.json(path + "log.json")
    logDF.write.parquet(path + "log.parquet")
    spark.close()
  }
}
