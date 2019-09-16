package companystaffmanagersystem

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer

object CompanyStaff {

  case class Person(name: String, age: Long)

  case class Score(n: String, score: Long)

  val personPath = "D:\\scalaWorkingSpace\\data\\people\\people.json"
  val scorePath = "D:\\scalaWorkingSpace\\data\\people\\peopleScores.json"

  /**
    * 数据分析，
    * 主要是对spark各种算子的熟悉
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("MovieAgePreferTopK").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val personsDF = spark.read.json(personPath)
    val scoresDF = spark.read.json(scorePath)

    val personsDS = personsDF.as[Person]
    val scoresDS = scoresDF.as[Score]

    println("使用groupBy算子分组：")
    val personsDSGrouped = personsDS.groupBy("name", "age").count()
    personsDSGrouped.show()


    println("使用agg算子concat内置函数，将姓名，年龄连接在一起，称为单个字符串列：")
    //    这块可能由于版本，跟书上略有不同
    personsDS.groupBy("name", "age")
      .agg(concat(column("name"), column("age")))
      .show()

    println("使用col算子来选择列")
    personsDS
      .joinWith(scoresDS, personsDS.col("name") === scoresDS.col("n"))
      .show()

    println("使用sum,avg,max,min,count,countDistinct,mean" +
      "等计算年龄总和，平均年龄，最大年龄，最小年龄，唯一年龄计数，平均年龄，当前时间等数据")
    personsDS.groupBy("name")
      .agg(
        sum("age"),
        avg("age"),
        max("age"),
        min("age"),
        count("age"),
        countDistinct("age"),
        mean("age"),
        current_date()
      ).show()


    println("函数collect_list,collect_set比较，函数collect_list包含可重复元素，collect_set不可重复：")
    personsDS.groupBy("name")
      .agg(collect_list("name"), collect_set("name"))
      .show()

    println("使用sample算子随机采样：")
    personsDS.sample(false, 0.5).show()

    println("使用randomSplit算子随机切分：")
    personsDS.randomSplit(Array(10, 20)).foreach(dataset => dataset.show())

    println("使用select算子选择列：")
    personsDS.select("name").show()

    println("使用joinWith关联人员与评分信息：")
    personsDS.joinWith(scoresDS, personsDS.col("name") === scoresDS.col("n"))
      .show()

    println("使用join关联人员与评分信息：")
    personsDS.join(scoresDS, personsDS.col("name") === scoresDS.col("n"))
      .show()

    println("使用sort算子对年龄降序：")
    personsDS.sort(desc("age")).show()


//    def myFlatMapFunction(myPerson:Person,myEncoder:Person):Dataset[Person]={personsDS}

    println("使用flatMap算子对数据进行转换，匹配如果名字为Andy，年龄加70，其他员工年龄加30")
    personsDS.flatMap(persons => persons match {
      case Person(name, age) if (name == "Andy") => List((name, age + 70))
      case Person(name, age) => List((name, age + 30))
    }).show()

    println("使用flatMap:")
    personsDS.mapPartitions { persons =>
      val result = ArrayBuffer[(String, Long)]()
      while (persons.hasNext) {
        val person = persons.next()
        result += ((person.name, person.age + 1000))
      }
      result.iterator
    }.show()

    println("使用dropDuplicates算子统计无重复姓名的人员记录：")
    personsDS.dropDuplicates("name").show()
    personsDS.distinct().show()

    println("使用reparttion算子设置分区：")
    println("原分区：" + personsDS.rdd.partitions.size)
    val repartitionDs = personsDS.repartition(4)
    println("repartition设置分区：" + repartitionDs.rdd.partitions.size)

    println("使用coalesce算子设置分区：")
    val coalesced: Dataset[Person] = repartitionDs.coalesce(2)
    println("coalesce设置分区数：" + coalesced.rdd.partitions.size)


    spark.stop()
  }

}
