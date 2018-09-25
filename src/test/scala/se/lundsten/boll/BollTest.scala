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

    val count = sparkSession.read
      .option("header", value = true)
      .csv("C:/git/boll/data/*/*")
      .withColumn("season", substring(split(input_file_name(), "/").getItem(8), 0, 9))
      .show(10, truncate = false)

//    printf("Count: " + count)
  }
}
