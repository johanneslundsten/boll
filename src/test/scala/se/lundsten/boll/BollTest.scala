package se.lundsten.boll

import org.apache.spark.sql.SparkSession
import org.junit.Test

class BollTest {

  @Test
  def test(): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val df = sparkSession.read
      .option("header", value = true)
      .csv("C:/git/boll/data/*/*")
      .withColumn("season", substring(split(input_file_name(), "/").getItem(8), 0, 9))
      .where($"div" === "SP1" && $"season" === "2017_2018")
      .withColumn("Date", to_timestamp($"Date", "dd/MM/yy"))
//      .printSchema()




    df.show(10, truncate = false)


  }
}
