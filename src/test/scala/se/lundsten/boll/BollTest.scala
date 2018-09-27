package se.lundsten.boll

import java.sql.Timestamp

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.junit.Test
import org.apache.spark.sql.functions._

class BollTest {

  @Test
  def test(): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    val team = "Barcelona"
    val opponent = "Real Madrid"

    val allGames = createAllGamesFromFile()

    val previousGames = allGames
      .where($"team" === team && $"opponent" === opponent)
      .show(10)

    val myFunc= udf[Int, Timestamp]({ s => {
      val pos : Int =  createTable(allGames
        .where($"date" < s))
        .where($"team" === team)
        .first().getAs("position")

      return pos
    }})

    createTable(allGames).show(100)

  }

  def createAllGamesFromFile() : DataFrame = {
    val sparkSession = SparkSession.builder()
      .getOrCreate()

    import sparkSession.implicits._

    val df = sparkSession.read
      .option("header", value = true)
      .csv("C:/git/boll/data/*/*")
      .withColumn("season", substring(split(input_file_name(), "/").getItem(8), 0, 9))
      .where($"div" === "SP1" && $"season" === "2017_2018")
      .withColumn("Date", to_timestamp($"Date", "dd/MM/yy"))
      .select(
        $"Div".as("league"),
        $"date",
        $"HomeTeam".as("home_team"),
        $"AwayTeam".as("away_team"),
        $"FTHG".as("home_goals"),
        $"FTAG".as("away_goals"),
        $"FTR".as("result"))
      .persist(StorageLevels.MEMORY_AND_DISK_2)
    //      .printSchema()


    val homeGames = df.select(
      $"home_team".as("team"),
      $"away_team".as("opponent"),
      $"home_goals".as("scored_goals"),
      $"away_goals".as("conceded_goals"),
      when($"result" === "H", 3).otherwise(when($"result" === "D", 1).otherwise(0)).as("points"),
      $"date",
      lit("home").as("home_or_away")
    )

    val awayGames = df.select(
      $"away_team".as("team"),
      $"home_team".as("opponent"),
      $"away_goals".as("scored_goals"),
      $"home_goals".as("conceded_goals"),
      when($"result" === "A", 3).otherwise(when($"result" === "D", 1).otherwise(0)).as("points"),
      $"date",
      lit("away").as("home_or_away"))

    val allGames = homeGames.union(awayGames)
      .persist(StorageLevels.MEMORY_AND_DISK_2)

    allGames
  }

  def createTable(games: DataFrame) : DataFrame = {

    val sparkSession = SparkSession.builder()
      .getOrCreate()

    import sparkSession.implicits._

    val w = Window
      .orderBy($"points".desc)

    return games
      .groupBy("team")
      .agg(
        sum("points").as("points"),
        sum($"scored_goals" - $"conceded_goals").as("GD"),
        sum($"scored_goals").as("scored"),
        sum($"conceded_goals").as("conceded")
      )
      .withColumn("position", row_number().over(w))
      .select("position", "team", "points", "scored", "conceded", "GD")
  }
}
